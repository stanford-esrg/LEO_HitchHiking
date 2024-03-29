''' 
Copyright 2023 The Board of Trustees of The Leland Stanford Junior University

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''

import pandas as pd
import subprocess
import tempfile
import time
from data_parse import aggregate_data
from bq_upload import upload_ping_file

def run_paris_trs(ip_file: str, output_file: str) -> None:
    """
    Run an ICMP paris-traceroute to every IP address in a given file.

    :param ip_file: file path string to a new-line delimited list of IPs to run traceroutes to
    :param output_file: file path string to .json file to output traceroute data
    """

    cmd_str = "scamper -O json -o " + output_file + " -c \"trace -P icmp-paris -q 1 \" " + ip_file
    try:
        subprocess.run(
                cmd_str, 
                shell=True, 
        )
    except ValueError:
        raise Exception("Invalid command: " + cmd_str)

def ttl_ping (sec_last: bool, input_file: str, output_destination: str, ttl: int, ping_len: int, ping_interval: int = 1, upload_to_bq: bool = False, bq_table_id: str = None) -> pd.DataFrame:
    """
    Run ping tests using ICMP paris-traceroute with first hop and max ttl are as specified.

    :param input_file: file path to ips to ping
    :param output_dir: file path to output data to
    :param ping_len: probecount, the number of probes to send
    :param ping_interval: number of seconds between each probe
    """

    processes = []
    output_dir = {}
    for seq in range(1, ping_len + 1):
        output_dir[seq] = scamper_output_file = tempfile.NamedTemporaryFile(mode='w+', suffix='.json')

    def append_data(process):
        p = process["pid"]

        # process isn't finished yet, keep in processes list
        if p.poll() is None:
            return True

        p.wait()
        output_dir[process["seq"]].seek(0)

        return False

    # create temporary output files then aggregate at the end
    for seq in range(1, ping_len + 1):
        start_time = time.time()
        scamper_output_file = output_dir[seq]
        scamper_output_file.flush()

        try:
            cmd_str = "scamper -O json -o " + scamper_output_file.name + " -c \"trace -P icmp-paris -q 1" + " -f " + str(ttl) + " -m " + str(ttl) + " \" " + input_file
            p = subprocess.Popen(
                cmd_str, 
                shell=True, 
            )
            processes.append({
                "pid": p,
                "seq": seq
            })
        except ValueError:
            continue
        processes = list(filter(append_data, processes))

        to_sleep = ping_interval - (time.time() - start_time)
        if to_sleep > 0:
            time.sleep(to_sleep)

    while len(processes) > 0:
        processes = list(filter(append_data, processes))

    df = aggregate_data(output_dir)

	# cleanup files
    for seq in range(1, ping_len + 1):
        output_dir[seq].close()

    if sec_last:
        print("len of sec_last_pings df: " + str(len(df)))
    else:
        print("len of last_pings df: " + str(len(df)))

    if upload_to_bq:
        with tempfile.NamedTemporaryFile(mode='w+') as temp_csv:
            temp_csv.flush()
            df.to_csv(temp_csv.name, header=False, index=False)
            temp_csv.seek(0)
            upload_ping_file(bq_table_id, temp_csv.name)
    else:
        df.to_csv(output_destination, header=None, index=None, mode='a')

    return df
