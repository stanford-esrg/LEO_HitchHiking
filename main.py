from data_collection import *

"""
Sample LEO HitchHiking script.

Queries Censys for exposed Starlink and OneWeb services. Traceroutes the exposed services
and pings reachable endpoints for 5 minutes. Data is uploaded to the specified BigQuery
dataset.
"""

def starlink_job():
    print("running starlink job")
    starlink_dc = DataCollection(bq_dataset_id="MY_BQ_DATASET") # FIXME: update with BQ dataset id
    starlink_df = starlink_dc.get_censys_exposed_services(14593, 4)
    tr_df = starlink_dc.paris_traceroute_exposed_services(starlink_df, 'ip', True) # change to False to save to file instead of BQ
    starlink_dc.ping_exposed_services(tr_df, 600, 1, True)  # change to False to save to file instead of BQ


def oneweb_job():
    print("running oneweb job")
    oneweb_dc = DataCollection(bq_dataset_id="MY_BQ_DATASET") # FIXME: update with BQ dataset id
    oneweb_df = oneweb_dc.get_censys_exposed_services(800, 4)
    tr_df = oneweb_dc.paris_traceroute_exposed_services(oneweb_df, 'ip', True)  # change to False to save to file instead of BQ
    oneweb_dc.ping_exposed_services(tr_df, 600, 1, True)  # change to False to save to file instead of BQ

if __name__ == "__main__":
    starlink_job()
    oneweb_job()
