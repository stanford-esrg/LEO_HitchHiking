import glob
import os
import pandas as pd
from multiprocessing import Process

def get_last_hops_from_paris_tr(file_path: str) -> pd.DataFrame:
    """
    Extract the hop number and IPs for the second-to-last and last hop in
    ICMP paris-traceroutes.

    :param file_path: file path to the .json formatted scamper trace output
    :return: dataframe with the IPs and hop numbers of the second-to-last and
    last hops in the traceroutes as well as the stop reason.
    """

    def sort_hops(e):
        return e['probe_ttl']

    def get_sec_last_ip(hops):
        hops.sort(key=sort_hops)
        return hops[-2]['addr']

    def get_sec_last_probe_ttl(hops):
        hops.sort(key=sort_hops)
        return hops[-2]['probe_ttl']

    df = pd.read_json(file_path, lines=True)

    # filter for only 'trace' data
    df = df[df["type"] == "trace"]
    df['sec_last_ip'] = df['hops'].apply(lambda x: get_sec_last_ip(x))
    df['sec_last_hop'] = df['hops'].apply(lambda x: get_sec_last_probe_ttl(x))
    df = df[['dst', 'stop_reason', 'hop_count', 'sec_last_ip', 'sec_last_hop']]
    return df

def aggregate_data(files: dict) -> pd.DataFrame:
    """
    Aggregates data from list of files containing scamper outputs when running ttl_ping
    into a single file.

    :param files: list of .json files from scamper output
    :return: a single aggregated dataframe with column for seq numbers
    """

    def get_date(start):
        try:
            return start['ftime'].split()[0]
        except:
            return None

    def get_start_time(start):
        try:
            return start['ftime']
        except:
            return None

    def get_start_sec(start):
        try:
            return start['sec']
        except:
            return None

    def get_rtt(hops):
        try: 
            return hops[0]['rtt']
        except:
            return None
    def get_probe_ttl(hops):
        try:
            return hops[0]['probe_ttl']
        except:
            return None
    def get_ip_at_ttl(hops):
        try:
            return hops[0]['addr']
        except:
            return None

    dfs = []
    for seq, f in files.items():
        f.flush()
        df = pd.DataFrame()
        try:
            df = pd.read_json(f.name, lines=True)
        except Exception as e:
            print("Could not load json file with seq: " + str(seq))
            print(e)

        if df.empty:
            continue
        df = df[df['type'] == 'trace']
        df['date'] = df['start'].apply(get_date)
        df['seq'] = [seq] * len(df)
        df['start_time'] = df['start'].apply(get_start_time)
        df['start_sec'] = df['start'].apply(get_start_sec)
        if 'hops' in df.columns:
            df['ip_at_ttl'] = df['hops'].apply(get_ip_at_ttl)
            df['probe_ttl'] = df['hops'].apply(get_probe_ttl)
            df['rtt'] = df['hops'].apply(get_rtt)
        else:
            df['ip_at_ttl'] = [None] * len(df)
            df['probe_ttl'] = [None] * len(df)
            df['rtt'] = [None] * len(df)
        df = df[['date', 'seq', 'dst', 'stop_reason', 'start_time', 'start_sec', 'hop_count', 'ip_at_ttl', 'probe_ttl', 'rtt']]
        dfs.append(df)

    if len(dfs) == 0:
        return pd.DataFrame(columns=['date',
            'seq',
            'dst',
            'stop_reason',
            'start_time',
            'start_sec',
            'hop_count',
            'ip_at_ttl',
            'probe_ttl',
            'rtt'])

    all_dfs = pd.concat(dfs)
    all_dfs.astype({
        'date': 'str', 
        'seq': 'int32', 
        'dst': 'str', 
        'stop_reason': 'str', 
        'start_time': 'str', 
        'start_sec': 'int32', 
        'hop_count': 'int32', 
        'ip_at_ttl': 'str', 
        'probe_ttl': 'float', 
        'rtt': 'float'
        }).dtypes

    return all_dfs

        

def agg_for_dir(dir, table_id):
    agg_dfs = []
    for f in glob.glob(os.path.join(dir, "*")):
        agg_dfs.append(aggregate_data(f, table_id))

    return agg_dfs
