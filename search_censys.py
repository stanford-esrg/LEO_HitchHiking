import errno
import json
import os
import pandas as pd
import subprocess
import sys
from censys.search import CensysCerts, CensysHosts
from datetime import date
from ipaddress import ip_address, IPv4Address, IPv6Address

def search_censys(asn: int, ipv: int = None):
    """
    Queries Censys for all exposed services (IP, port) for the specified ASN.

    Starlink ASN: 14593
    Oneweb ASN: 800

    :param asn: the autonomous system number
    :param ipv: (optional) specify 4 or 6 to filter for IP version 
    (default is no filter)
    :return: list of exposed ip/port
    """ 

    print("(search_censys) start")
    h = CensysHosts()
    c = CensysCerts()
    exposed_services = {
        'date': [],
        'asn': [],
        'ip': [],
        'port': [],
        'dns_name': [], 
        'pep_link': [],
    }

    certs_queried = {}

    # search Censys for services matching asn
    for page in h.search("autonomous_system.asn:" + str(asn), pages=-1):
        for entry in page:
            try:
                for service in entry['services']:
                    # filter IPv if specified
                    if (((ipv == 4) and type(ip_address(entry['ip'])) is not IPv4Address) or 
                        ((ipv == 6) and type(ip_address(entry['ip'])) is not IPv6Address)):
                        continue

                    # get DNS name if it exists
                    dns_name = None
                    # FIXME
                    try: 
                        dns_name = entry['dns']['reverse_dns']['names']
                    except:
                        dns_name = None

                    # label hosts using pep-link
                    pep_link = None
                    # try:
                    #     # services.tls.certificates.leaf_data.subject_dn
                    #     certificate = service['certificate']
                    #     cert = ""
                    #     if certificate in certs_queried:
                    #         cert = certs_queried[certificate]
                    #     else:
                    #         cert = c.raw_search(certificate)
                    #         certs_queried[certificate] = cert
                    #         print(certificate)

                    #     subject_dn = cert['result']['hits'][0]['parsed']['subject_dn']
                    #     # print(subject_dn) #FIXME
                    #     if 'peplink' in subject_dn.lower():
                    #         pep_link = True
                    # except:
                    #     pep_link = False

                    exposed_services['asn'].append(asn)
                    exposed_services['ip'].append(entry['ip'])
                    exposed_services['port'].append(service['port'])
                    exposed_services['dns_name'].append(dns_name)
                    exposed_services['pep_link'].append(pep_link)

            except Exception as e:
                sys.stderr.write(str(e) + "\t could not get ip")
                sys.stderr.write(str(entry))

    exposed_services['date'] = [str(date.today())] * len(exposed_services['ip'])
    print("(search_censys) end")
    return exposed_services

# # paris-traceroute
# def run_paris_trs(ip_file, output_file):
#     """
#     output file json
#     """
#     # cmd_str = "scamper -O json -o /mnt/darknet_proj/mandat_scratch/satellite/satellite_measurements/scamper/2023-05-10/paris_icmp_20/" + str(file.split('/')[-1]) + "_" + str(tr_n) + ".json -c \"trace -P icmp-paris -q 20 -Q \" " + file
#     cmd_str = "scamper -O json -o " + output_file +  " -c \"trace -P icmp-paris -q 1 \" " + ip_file
#     subprocess.run(cmd_str, shell=True)

# # extract last hop number, second-to-last hop number, second-to-last ip
# def sort_hops(e):
#     return e['probe_ttl']

# def get_sec_last_ip(hops):
#     hops.sort(key=sort_hops)
#     return hops[-2]['addr']

# def get_sec_last_probe_ttl(hops):
#     hops.sort(key=sort_hops)
#     return hops[-2]['probe_ttl']
