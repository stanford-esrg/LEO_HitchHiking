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

    # certs_queried = {}

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
                    dns_name = []
                    # FIXME
                    try: 
                        dns_name = entry['dns']['reverse_dns']['names']
                    except:
                        dns_name = []

                    # label hosts using pep-link 
                    # (commented out because costly in # of queries)
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
    return exposed_services
