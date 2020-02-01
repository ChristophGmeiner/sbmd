# Gathering and Analyzong Traffic and Delay Data for the Greater Munich Area

This git repo shows a way on how to gather structured and long-term traffic analytical information of the Greater Munich area. Thos contains data about many train connections, car connections and weather data.

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
If you choose other packages than Schiene, you could also expand the proces to all areas in the world


## Files

All confidential AWS data is stored in a local config file and loaded to the cripts via configparser.

### sbahnmuc02.py
The relevant train stations are defind here and transfered to iterables and files.

### sbahnmuc03.py
This script gathers data from the trains.

### sbahnmuc04_gmap.py
This script gathers the date from Google map API. Due tothe fact this API gets very expensive the number of stations or start/destination combinations are reduced.

### sbahnmuc05_weather.py
This script loads the weather data.

### zz01_startVM1.py & zz02_StopVM1.py
This scripts start or stop the productive VM
