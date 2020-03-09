from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class sbmd_plugin(AirflowPlugin):
    name = "sbmd_plugin"
    operators = [
            operators.RunGlueCrawlerOperator
            ]
    helpers = [
        helpers.CreateTables,
        helpers.InsertTables
    ]

