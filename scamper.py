import json
import os
import subprocess
import time

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

def ttl_ping (input_file: str, output_dir: list, ttl: int, ping_len: int, ping_interval: int = 1) -> None:
    """
    Run ping tests using ICMP paris-traceroute with first hop and max ttl are as specified.

    :param input_file: file path to ips to ping
    :param output_dir: file path to output data to
    :param ping_len: probecount, the number of probes to send
    :param ping_interval: number of seconds between each probe
    """

    def append_data(process):
        p = process["pid"]

        # process isn't finished yet, keep in processes list
        if p.poll() is None:
            return True

        p.wait()
        return False

    processes = []
    # create temporary output files then aggregate at the end
    for seq in range(1, ping_len + 1):
        start_time = time.time()
        output_file = output_dir[seq - 1] #os.path.join(output_dir, str(seq) + ".json")
        try:
            cmd_str = "scamper -O json -o " + output_file.name + " -c \"trace -P icmp-paris -q 1" + " -f " + str(ttl) + " -m " + str(ttl) + " \" " + input_file
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
       