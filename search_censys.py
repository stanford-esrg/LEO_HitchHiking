import json
import sys
from censys.search import CensysHosts
from ipaddress import ip_address, IPv4Address, IPv6Address

def search_censys(asn: int, output_file: str = None, ipv = None):
	"""
	Queries Censys for all exposed services (IP, port) for the specified ASN.

	Starlink ASN: 14593
	Oneweb ANS: 800

    :param asn: the autonomous system number
    :param output_file: (optional) output_file for json formatted data
    :param ipv: (optional) specify 4 or 6 to filter for IP version 
	(default is no filter)
    :return: list of exposed ip/port
    """ 

	h = CensysHosts()
	exposed_services = []

	# search Censys for services matching asn
	for page in h.search("autonomous_system.asn:" + str(asn), pages=-1):
		for entry in page:
			try:
				for service in entry['services']:
					# filter IPv if specified
					if (((ipv == 4) & type(ip_address(entry['ip'])) is not IPv4Address) or 
						((ipv == 6) & type(ip_address(entry['ip'])) is not IPv6Address)):
						continue

					exposed_services.append({
						'ip': entry['ip'],
						'port': service['port']
					})
			except Exception as e:
				sys.stderr.write(str(e) + "\t could not get ip")
				sys.stderr.write(entry)

	# output to json file if specified
	if output_file:
		with open(output_file, "w") as f:
			json.dump(exposed_services, f)

	return exposed_services