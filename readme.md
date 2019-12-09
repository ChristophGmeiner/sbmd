# Gathering Delay Data of Munich S-Bahn

This git repo shows a way on how to gather structured and long-term delay information of the Munich S Bahn (suburban trains) for two pre-defined connections (these connections can be defined by the user).

This is mainly done by the schiene python module (for more information on that, please see here: https://github.com/kennell/schiene/blob/master/README.md). \
Thanks for the great package to the developers.

The data is derived with the Schiene packgae and stored in local instance of MongoDB. I chose MongoDB since it is for me the easiest way for loading python dicts into a database.
But with some modifications the retrieved dicts can easily be transformed to pandas dataframes and in this way loaded into a relational database as well.

The purpose of this gathering is to get more transparency on the average delay for e.g. on your daily commute.

Since the schiene module is working for all DB (Deutsche Bahn) connections the use of this repo is not limited on Munich.


## Files

### sbahnmuc01.ipynb
This notebook shows the basic functionality.

### sbahnmuc01.py
This is a nearly production - ready script. Since the DeutscheBahn website shows only data on delays for current connections, it is the best way to schedule this script every 5 minutes for always having the latest information.

Instead of hard-coding the to and from destiantions, one could also work with rwa inputs from shell here.
