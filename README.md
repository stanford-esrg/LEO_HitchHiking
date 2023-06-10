# LEO HitchHiking To Measure LEO Satellite Latency

HitchHiking is a methodology to measure LEO satellite network characteristics at scale. 
This repository is an implementation of HitchHiking to measure the latency of Starlink and OneWeb customer services. 
We measure latency by continually sending TTL limited pings on hitchhiked LEO links and collecting their round trip time.
To learn more about HitchHiking system and performance, check out this [paper]().

The pipeline is run on a daily basis from Stanford University.
The data collected is publically available in the following [Google BigQuery](https://cloud.google.com/bigquery/docs/introduction) tables: [`satellite-measurement-386900.starlink`](console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1ssatellite-measurement-386900!2sstarlink) and [`satellite-measurement-386900.oneweb`](console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1ssatellite-measurement-386900!2soneweb). 


## Requirements
HitchHiking does not require a satellite dish. 
Rather, this pipeline requires [Censys](https://github.com/censys/censys-python) (manual [here](https://censys-python.readthedocs.io/en/stable/)) to find exposed LEO customer services, and [Scamper](https://www.caida.org/catalog/software/scamper/man/scamper.1.pdf) (manual [here](https://www.caida.org/catalog/software/scamper/man/scamper.1.pdf)) to scan LEO customer services. 

To install Censys:
```
pip install censys
```
and follow the rest of the instructions [here](https://github.com/censys/censys-python).

To install Scamper: 

```
sudo apt update
sudo apt install scamper
```

## Usage

To run the pipeline:

```
python main.py
```

To collect data over time, schedule the pipeline to run the pipeline on an automated schedule (e.g., in a [cron job](https://man7.org/linux/man-pages/man5/crontab.5.html)). 


## License and Copyright

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
