from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import helpers

# Defining the plugin class
class CGPlugins(AirflowPlugin):
    name = "CGPlugins"

    helpers = [
        helpers.sbahnmuc02,
        helpers.sbahnmuc03,
        helpers.sbhanmuc04_gmaps,
        helpers.sbahnmuc05_weather        
    ]