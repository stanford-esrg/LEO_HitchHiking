import ast
import os
import pandas as pd
import tempfile
from datetime import date
from data_parse import get_last_hops_from_paris_tr
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

        
    def get_censys_exposed_services(self, asn: int, ipv: int = None) -> pd.DataFrame:
        """
        Queries Censys for exposed services and returns the result as a dataframe.

        :param asn: the autonomous system number to query
        :param ipv: (optional) specify 4 or 6 to filter for IP version
        :return: dataframe of exposed services information
        """
        def stringified_list_to_list(x):
            if x == None:
                return []
            try:
                return ast.literal_eval(x)
            except:
                return [x]
            
        # temp to appease bigquery json file requirements
        def list_of_nulls_to_empty_list(x):
            return []

        df = pd.DataFrame()
        exposed_services = {}
        try:
            exposed_services = search_censys(asn, ipv)
            df = pd.DataFrame.from_dict(exposed_services)
        except:
            return df
        df['dns_name'] = df['dns_name'].apply(str)
        df = df.groupby(['ip', 'date', 'asn', 'dns_name']).agg(list).reset_index()
        df['dns_name'] = df['dns_name'].apply(stringified_list_to_list)
        df['pep_link'] = df['pep_link'].apply(list_of_nulls_to_empty_list)
        return df

    def paris_traceroute_exposed_services(self, df: pd.DataFrame, ip_col: str, upload_to_bq: bool = True) -> pd.DataFrame:
        """
        Queries Censys for exposed services then runs an icmp paris-traceroute
        to each exposed IP address and stores data in `exposed_services`.

        :param df: a dataframe containing at least one column that contains IP addresses to traceroute
        :param ip_col: the name of the column that contains the IP addresses to traceroute
        :param upload_to_bq: (optional) upload data to big query (default saves the output to file)
        :return: dataframe of traceroute results
        """

        output_file = os.path.join(self.exposed_services_dir, str(date.today()) + ".json")

        # run paris-traceroutes for each unique IP and extract the 
        # second-to-last hop and last hop

        fallback_file = False
        with tempfile.NamedTemporaryFile(mode='w+') as temp_ip:
            # Uncomment the following block if using a fallback file incase Censys queries
            # fail. 
            # If the provided dataframe does not contain data, use a fallback file

            # if df.empty:
            #     print("(paris_traceroute_exposed_services) using fallback file")
            #     df = pd.read_json(
            #         'MYFALLBACKFILE.json' # TODO: edit to fallback file path
            #         lines=True
            #     )
            #     df = df[['ip', 'date', 'asn', 'dns_name', 'port', 'pep_link']]
            #     fallback_file = True
                
            df[[ip_col]].to_csv(temp_ip.name, header=False, index=False) 
            with tempfile.NamedTemporaryFile(mode='w+') as temp_tr:

                run_paris_trs(temp_ip.name, temp_tr.name)
                last_hops = get_last_hops_from_paris_tr(temp_tr.name)
                df = df.merge(
                    last_hops, 
                    how="left", 
                    left_on=ip_col, 
                    right_on='dst'
                )
                df = df.drop(columns=['dst'])
        
        # output to json file
        if upload_to_bq and not fallback_file:
            with tempfile.NamedTemporaryFile(mode='w+', suffix=".json") as temp_json:
                df.to_json(temp_json.name, orient="records", lines=True)
                temp_json.seek(0)
                upload_exposed_services_file(self.bq_exposed_services_table_id, temp_json.name)
        elif not fallback_file:
            df.to_json(output_file, orient="records", lines=True)

        return df


    def ping_exposed_services(self, df: pd.DataFrame, ping_len: int = 5, ping_interval: int = 1, upload_to_bq: bool = False) -> None:
        """
        Pings the exposed services and collects measurements for the RTTs of 
        the last hop and the second-to-last hop found in the paris-traceroute. 
        Only collects measurements for exposed services with a completed 
        traceroute.

        :param df: dataframe constructed from `paris_traceroute_exposed_services`
        :param ping_len: (optional) specify the number of probes to send
        :param ping_interval: (optional) specify the number of seconds between probes
        :param upload_to_bq: (optional) upload data to bigquery (default saves output to file)
        """

        # only ping the reachable endpoints
        df = df[df['stop_reason'] == 'COMPLETED']
        df = df.groupby(['hop_count', 'sec_last_hop'])['ip'].agg(list).reset_index()

        sec_last_hop_output = os.path.join(self.pings_dir['sec_last_hop'], str(date.today()) + ".csv" )
        last_hop_output = os.path.join(self.pings_dir['last_hop'], str(date.today()) + ".csv")
        
        # create temporary file structure
        temp_ip_files = {}

        for i in df.index:
            temp_ip_files[i] = tempfile.NamedTemporaryFile(mode='w+', suffix='.txt')

        for i in df.index:
            ips = df.iloc[i]['ip']
            sec_last_ttl = int(df.iloc[i]['sec_last_hop'])
            last_ttl = int(df.iloc[i]['hop_count'])

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

        # clean up temporary file structure
        for i in df.index:
            temp_ip_files[i].close()
