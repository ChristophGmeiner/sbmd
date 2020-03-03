# Gathering and Analyzong Traffic and Delay Data for the Greater Munich Area

This git repo shows a way on how to gather structured and long-term traffic analytical information of the Greater Munich area. This contains data about many train and car connections and weather data.
This is also my Capstone project for my Udacity Nanodegree in Data Engineering.

This is mainly done by the schiene python module (for more information on that, please see here: https://github.com/kennell/schiene/blob/master/README.md). \
Thanks for the great package to the developers.

Also the googlemaps and the pyowm is used for traffic and weather data.
Please find details on that here:
https://github.com/googlemaps/
https://github.com/csparpa/pyowm
Also here: Thanks for the great and very helpful packages.

You'll need API keys for gathering data from these two APIs. On the specific websites the ways for getting the API keys are described.
**Please watch out: In some cases (depending on the volume of requests) these services can get very expensive!**

In all cases the data is stored as json files in separate AWS S3 buckets.

Since all of the mentioned above packages work also in areas apart from the Greater Munich area, this process would also work in other parts of Germany or Europe.
If you choose other packages than Schiene, you could also expand the proces to all areas in the world.

# Process and data pipeline

The data pipeline consists of three steps

## Data gathering
This process involves mainly web scrapping via the mentioned above APIs and storing each API request as a json files in different S3 buckets (one for train, one for car and another one for weather data). This is carried out on an AWS EC2 and is scheduled via crontab. Also the start and stop of the EC2 is scheduled on another (very low cost) EC2, which is always on.

This process of couse could also be carried out with Apache Airflow. Since the EC2 I rented was to weak for managing many parallel processes, this was not set productive.

I chose S3 and the json format, since it is the most natural way to store this data (since the API request result in single a single json file per record). In this way the raw data can be stored flexible and marked accordingly when loaded to the database. Therfore I have perfect control, what data was already loaded, what was not yet loaded and when what was loaded.
Also concerning costs it is - according to my current knowledge - the most sufficient way for this process.

### Relevant files

All confidential data is stored in a local config file and loaded to the cripts via configparser.

#### sbahnmuc02.py
The relevant train stations are defind here and transfered to iterables and files.

#### sbahnmuc03.py
This script gathers data from the trains.

#### sbahnmuc04_gmap.py
This script gathers the date from Google map API. Due tothe fact this API gets very expensive the number of stations or start/destination combinations are reduced.

#### sbahnmuc05_weather.py
This script gathers the weather data.


## Data transferring
This process is about aggregating and transforming the json data from the step before and transfer it into Postgres tables (running on a AWS RDS service).

In a first step the database gets created from a snapshot. For the initial load the live tables will be created. 
After that - using the first three scripts below - the data is transformed to a dataframa, bulk copied to a postgres staging table, inserted into the respective live tables and finally all transferred json files are archived into a newly created folder in the respective S3 bucket.

This process of couse could also be carried out with Apache Airflow. Since the EC2 I rented was to weak for managing many parallel processes, this was not set productive.
Nevertheless I created a dag here for demonstration purposes, but due to cost restrictions it never went productive.

I chose postgres here, since the data should be stored in a relational way. Since the data is currently not that big and proper indexed AWS Redshift or another MPP database is not necessary.
Of course,m when the datra should be scaled - e.g. to all areas in Germany - postgres would not be sufficient anymore. Than maybe Appache Cassandra or a direct transfer to AWS redshift should be preferred.

### Relevant files

All confidential data is stored in a local config file and loaded to the cripts via configparser.
The three files below transfer the transformed json data to postgres staging tables, respectively for train, car and weather data.

#### sbahnmuc03b_TransferDB.py
#### sbahnmuc04b_TransferDB.py
#### sbahnmuc05b_TransferDB.py
#### zz07_Transfer_DB_Data.sh

This summarizes the three scripts abover in a shell script.

#### zz09_InsertLiveTables.py

This transfer4s data in postgres from staging to love tables.

#### airflow folder
This folder contains all necessary files for the Airflow dag.


## Data Modelling 

## Tables in Postgres Database


## Other Files

All confidential AWS data is stored in a local config file and loaded to the cripts via configparser.



### zz01_startVM1.py, zz02_StopVM1.py, zz01b_bash.sh, zz02b_bash.sh
This scripts start or stop the productive VM
