import ast
import os
import pandas as pd
import tempfile
import time
from datetime import date
from data_parse import get_last_hops_from_paris_tr, aggregate_data
from multiprocessing import Process
from scamper import *
from search_censys import *
from bq_upload import *


class DataCollection:

    def __init__(self, data_dir: str = None, bq_dataset_id: str = None) -> None:
        """
        Specifies the directory to store measurement data in. If one is not
        specified, the current directory is used.

        Specifies the Big Query table to upload to.

        :param data_dir: directory path
        :param bq_dataset_id: Big Query dataset ID
        """ 
        if data_dir is None:
            data_dir = "."
        if os.path.exists(data_dir):
            self.data_dir = data_dir
            self.create_data_dirs()
        else:
            raise Exception("Directory path provided does not exist.")

        if bq_dataset_id is None:
            self.bq_exposed_services_table_id = None
            self.bq_sec_last_ping_table_id = None
            self.bq_last_ping_table_id = None
        else:
            self.bq_exposed_services_table_id = bq_dataset_id + ".exposed_services"
            self.bq_sec_last_ping_table_id = bq_dataset_id + ".sec_last_pings"
            self.bq_last_ping_table_id = bq_dataset_id + ".endpoint_pings"
            

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

        
    def paris_traceroute_exposed_services(self, asn: int, ipv: int = None, upload_to_bq: bool = True) -> pd.DataFrame:
        """
        Queries Censys for exposed services then runs an icmp paris-traceroute
        to each exposed IP address and stores data in `exposed_services`.

        :param asn: the autonomous system number to query
        :param ipv: (optional) specify 4 or 6 to filter for IP version
        :param upload_to_bq: (optional) upload data to big query (default saves the output to file)
        :return: dataframe of exposed services information
        """

        print("(paris_traceroute_exposed_services) start")
        def stringified_list_to_list(x):
            if x == None:
                return []
            try:
                return ast.literal_eval(x)
            except:
                return [x]
            
        # FIXME - temp to appease bigquery json file requirements
        def list_of_nulls_to_empty_list(x):
            return []

        output_file = os.path.join(self.exposed_services_dir, str(date.today()) + ".json")

        # search Censys for exposed services matching `asn`
        # FIXME
        df = pd.DataFrame()
        exposed_services = {}
        fallback_file = False
        try:
            exposed_services = search_censys(asn, ipv)
            print("(paris_traceroute_exposed_services) num of services found: " + str(len(exposed_services['ip'])))
            df = pd.DataFrame.from_dict(exposed_services)
        except:
            print('(paris_traceroute_exposed_services) using fallback file')
            df = pd.read_json(
                '/mnt/darknet_proj/mandat_scratch/satellite/satellite_measurements/exposed_services/2023-05-17.json',
                lines=True
            )
            return df
        df['dns_name'] = df['dns_name'].apply(str)
        df = df.groupby(['ip', 'date', 'asn', 'dns_name']).agg(list).reset_index()
        df['dns_name'] = df['dns_name'].apply(stringified_list_to_list)
        df['pep_link'] = df['pep_link'].apply(list_of_nulls_to_empty_list)
        
        # run paris-traceroutes for each unique IP and extract the 
        # second-to-last hop and last hop
        with tempfile.NamedTemporaryFile(mode='w+') as temp_ip:
            df[['ip']].to_csv(temp_ip.name, header=False, index=False) 
            with tempfile.NamedTemporaryFile(mode='w+') as temp_tr:

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
        if upload_to_bq and not fallback_file:
            with tempfile.NamedTemporaryFile(mode='w+', suffix=".json") as temp_json:
                df.to_json(temp_json.name, orient="records", lines=True)
                temp_json.seek(0)
                upload_exposed_services_file(self.bq_exposed_services_table_id, temp_json.name)
        else:
            df.to_json(output_file, orient="records", lines=True)

        print("(paris_traceroute_exposed_services) end")
        return df


    def ping_exposed_services(self, asn: int, ipv: int = None, ping_len: int = 5, ping_interval: int = 1, upload_to_bq: bool = False) -> None:
        """
        Pings the exposed services and collects measurements for the RTTs of 
        the last hop and the second-to-last hop found in the paris-traceroute. 
        Only collects measurements for exposed services with a completed 
        traceroute.

        :param asn: the autonomous system number to query
        :param ipv: (optional) specify 4 or 6 to filter for IP version
        :param ping_len: (optional) specify the number of probes to send
        :param ping_interval: (optional) specify the number of seconds between probes
        :param upload_to_bq: (optional) upload data to bigquery (default saves output to file)
        """

        print("(paris_exposed_services) start")
        start_time = time.time()
        # # FIXME
        # df = pd.read_json("exposed_services/2023-05-18.json", lines=True)
        df = self.paris_traceroute_exposed_services(asn, ipv, upload_to_bq)
        print("(paris_exposed_services) total exposed IPs: " + str(len(df)))
        
        # only ping the reachable endpoints
        df = df[df['stop_reason'] == 'COMPLETED']
        print("(paris_exposed_services) filtered exposed IPs: " + str(len(df)))
        df = df.groupby(['hop_count', 'sec_last_hop'])['ip'].agg(list).reset_index()
        print("(paris_exposed_services) grouped exposed IPs: " + str(len(df)))

        sec_last_hop_output = os.path.join(self.pings_dir['sec_last_hop'], str(date.today()) + ".csv" )
        last_hop_output = os.path.join(self.pings_dir['last_hop'], str(date.today()) + ".csv")
        
        # create temporary file structure
        temp_ip_files = {}

        # agg_sec_last_hop_data = tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv')
        # agg_last_hop_data = tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv')
        for i in df.index:
            temp_ip_files[i] = tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.txt')

        for i in df.index:
            print("--- %s seconds ---" % (time.time() - start_time))
            ips = df.iloc[i]['ip']
            sec_last_ttl = int(df.iloc[i]['sec_last_hop'])
            last_ttl = int(df.iloc[i]['hop_count'])
            print("pinging ips with \t sec_last_hop: " + str(sec_last_ttl) + "\tlast_ttl: " + str(last_ttl))
            print("num IPs in ping: " + str(len(ips)))


            ip_df = pd.DataFrame({'ip': ips})

            temp_ip_files[i].flush()
            ip_df.to_csv(temp_ip_files[i].name, header=False, index=False) 
            temp_ip_files[i].seek(0)


            p1 = Process(target = ttl_ping,
                        args = (True, temp_ip_files[i].name, 
                                sec_last_hop_output, sec_last_ttl, 
                                ping_len, ping_interval, 
                                upload_to_bq, self.bq_sec_last_ping_table_id))
            p1.start()

            temp_ip_files[i].seek(0)

            p2 = Process(target = ttl_ping,
                        args = (False, temp_ip_files[i].name, 
                                last_hop_output, last_ttl, 
                                ping_len, ping_interval, 
                                upload_to_bq, self.bq_last_ping_table_id))
            p2.start()

            p1.join()
            p2.join()

            
        print("(paris_exposed_services) clean up")
        # clean up temporary file structure
        for i in df.index:
            temp_ip_files[i].close()
        # agg_sec_last_hop_data.close()
        # agg_last_hop_data.close()

        print("(paris_exposed_services) end")
        print("--- TOTAL TIME %s seconds ---" % (time.time() - start_time))
