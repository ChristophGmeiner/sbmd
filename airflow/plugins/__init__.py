from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "sbmd_plugins"
    operators = [
            operators.RunGlueCrawlerOperator,
            operators.ModifyRDSPostgres,
            operators.CSV_S3_PostgresOperator
            ]
    helpers = [
        helpers.CreateTables,
        helpers.InsertTables
    ]

