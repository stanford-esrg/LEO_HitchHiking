import ast
import json
import glob
import os
import pandas as pd
import subprocess
import tempfile
import time
from datetime import date
from data_parse import get_last_hops_from_paris_tr, aggregate_data
from multiprocessing import Process
from scamper import *
from search_censys import *

class DataCollection:

    def __init__(self, data_dir: str = None) -> None:
        """
        Specifies the directory to store measurement data in. If one is not
        specified, the current directory is used.

        :param data_dir: directory path
        """ 
        if data_dir is None:
            data_dir = "."
        if os.path.exists(data_dir):
            self.data_dir = data_dir
            self.create_data_dirs()
        else:
            raise Exception("Directory path provided does not exist.")

    def create_data_dirs(self) -> None:
        """
        Creates directories to store measurement data in the specified directory.
        `exposed_services` stores information about the exposed ips ports 
        `pings` stores ping tests
        """ 
        exposed_services_path = os.path.join(self.data_dir, "exposed_services")
        if not os.path.exists(exposed_services_path):
            os.makedirs(exposed_services_path)
        self.exposed_services_dir = exposed_services_path

        pings_path = os.path.join(self.data_dir, "pings")
        if not os.path.exists(pings_path):
            os.makedirs(pings_path)
        pings_sec_last_path = os.path.join(pings_path, "sec_last")
        if not os.path.exists(pings_sec_last_path):
            os.makedirs(pings_sec_last_path)
        pings_last_path = os.path.join(pings_path, "last")
        if not os.path.exists(pings_last_path):
            os.makedirs(pings_last_path)
        self.pings_dir = {
            "sec_last_hop": pings_sec_last_path,
            "last_hop": pings_last_path
        }

        
    def paris_traceroute_exposed_services(self, asn: int, ipv: int = None, output_to_file: bool = False) -> pd.DataFrame:
        """
        Queries Censys for exposed services then runs an icmp paris-traceroute
        to each exposed IP address and stores data in `exposed_services`.

        :param asn: the autonomous system number to query
        :param ipv: (optional) specify 4 or 6 to filter for IP version
        :param output_to_file: (optional) save results to .json file (default does not output to file)
        :return: dataframe of exposed services information
        """

        print("(paris_traceroute_exposed_services) start")
        def stringified_list_to_list(x):
            try:
                return ast.literal_eval(x)
            except:
                return [x]

        output_file = os.path.join(self.exposed_services_dir, str(date.today()) + ".json")

		# search Censys for exposed services matching `asn`
        exposed_services = search_censys(asn, ipv)
        print("(paris_traceroute_exposed_services) num of services found: " + str(len(exposed_services['ip'])))
        df = pd.DataFrame.from_dict(exposed_services)
        df['dns_name'] = df['dns_name'].apply(str)
        df = df.groupby(['ip', 'date', 'asn', 'dns_name']).agg(list).reset_index()
        df['dns_name'] = df['dns_name'].apply(stringified_list_to_list)
        
        # run paris-traceroutes for each unique IP and extract the 
        # second-to-last hop and last hop
        with tempfile.NamedTemporaryFile() as temp_ip:
            df[['ip']].to_csv(temp_ip, header=False, index=False) 
            with tempfile.NamedTemporaryFile() as temp_tr:

                print("(paris_traceroute_exposed_services) run_paris_trs start")
                run_paris_trs(temp_ip.name, temp_tr.name)
                print("(paris_traceroute_exposed_services) run_paris_trs end")
                last_hops = get_last_hops_from_paris_tr(temp_tr.name)
                df = df.merge(
                    last_hops, 
                    how="left", 
                    left_on='ip', 
                    right_on='dst'
                )
                df = df.drop(columns=['dst'])
        
        # output to json file
        if output_to_file:
            df.to_json(output_file,
            orient="records",
            lines=True)
        print("(paris_traceroute_exposed_services) end")
        return df


    def ping_exposed_services(self, asn: int, ipv: int = None, ping_len: int = 5, ping_interval: int = 1, output_to_file = False) -> None:
        """
        Pings the exposed services and collects measurements for the RTTs of 
        the last hop and the second-to-last hop found in the paris-traceroute. 
        Only collects measurements for exposed services with a completed 
        traceroute.

        :param asn: the autonomous system number to query
        :param ipv: (optional) specify 4 or 6 to filter for IP version
        :param ping_len: (optional) specify the number of probes to send
        :param ping_interval: (optional) specify the number of seconds between probes
        :param output_to_file: (optional) save results to .json file (default does not output to file)
        """

        print("(paris_exposed_services) start")
        start_time = time.time()
        df = self.paris_traceroute_exposed_services(asn, ipv, output_to_file)
        print("(paris_exposed_services) total exposed IPs: " + str(len(df)))
        
        # only ping the reachable endpoints
        df = df[df['stop_reason'] == 'COMPLETED']
        print("(paris_exposed_services) filtered exposed IPs: " + str(len(df)))
        df = df.groupby(['hop_count', 'sec_last_hop'])['ip'].agg(list).reset_index()
        print("(paris_exposed_services) grouped exposed IPs: " + str(len(df)))

        
        # create temporary file structure
        temp_ip_files = {}
        temp_sec_last_hop_files = {}
        temp_last_hop_files = {}
        processes = []

        agg_sec_last_hop_data = tempfile.NamedTemporaryFile(delete=False)
        agg_last_hop_data = tempfile.NamedTemporaryFile(delete=False)
        for i in df.index:
            temp_ip_files[i] = tempfile.NamedTemporaryFile(delete=False)
            temp_sec_last_hop_files[i] = [tempfile.NamedTemporaryFile(delete=False)] * ping_len
            temp_last_hop_files[i] = [tempfile.NamedTemporaryFile(delete=False)] * ping_len

        for i in df.index:
            print("--- %s seconds ---" % (time.time() - start_time))
            ips = df.iloc[i]['ip']
            sec_last_ttl = int(df.iloc[i]['sec_last_hop'])
            last_ttl = int(df.iloc[i]['hop_count'])
            print("pinging ips with \t sec_last_hop: " + str(sec_last_ttl) + "\tlast_ttl: " + str(last_ttl))
            print("num IPs in ping: " + str(len(ips)))
            ip_df = pd.DataFrame({'ip': ips})
            print(ip_df)

            ip_df.to_csv(temp_ip_files[i], index=False) 
            temp_ip_files[i].seek(0)

            p1 = Process(target = ttl_ping,
                        args = (temp_ip_files[i].name, temp_sec_last_hop_files[i], sec_last_ttl, ping_len, ping_interval,))
            p1.start()
            processes.append(p1)
            p2 = Process(target = ttl_ping,
                        args = (temp_ip_files[i].name, temp_last_hop_files[i], last_ttl, ping_len, ping_interval,))
            p2.start()
            processes.append(p2)


        for p in processes:
            p.join()

		# aggregate data
        for i in df.index:
            aggregate_data(temp_sec_last_hop_files[i]).to_csv(agg_sec_last_hop_data.name, header=None, index=None, mode='a') 
            aggregate_data(temp_last_hop_files[i]).to_csv(agg_last_hop_data.name, header=None, index=None, mode='a')

		# output to a csv file
        if output_to_file:
            print("(paris_exposed_services) outputting to file")
            sec_last_hop_output = os.path.join(self.pings_dir['sec_last_hop'], str(date.today()) + ".csv" )
            last_hop_output = os.path.join(self.pings_dir['last_hop'], str(date.today()) + ".csv")
            with open(sec_last_hop_output, 'w') as of:
                with open(agg_sec_last_hop_data.name, 'r') as rf:
                    of.write(rf.read())
            with open(last_hop_output, 'w') as of:
                with open(agg_last_hop_data.name, 'r') as rf:
                    of.write(rf.read())
            
        print("(paris_exposed_services) clean up")
		# clean up temporary file structure
        for i in df.index:
            temp_ip_files[i].close()
            for x in temp_sec_last_hop_files[i]:
                x.close()
            for x in temp_last_hop_files[i]:
                x.close()
        agg_sec_last_hop_data.close()
        agg_last_hop_data.close()

        print("(paris_exposed_services) end")
        print("--- TOTAL TIME %s seconds ---" % (time.time() - start_time))
