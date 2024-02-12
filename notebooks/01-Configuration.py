# Databricks notebook source
# NOTEBOOK OVERVIEW
# This is the notebook that houses the configuration values used across the entire accelerator. The workflow is orchestrated through the use of the %run command in subsequent notebooks.

# COMMAND ----------

import json

# Define configurations
config = {
    "catalog": "hr_documentation",
    "schema": "employee_handbook",
    "volume": "hr_docs",
    "medallion-layer-1": "bronze",
    "medallion-layer-2": "silver",
    "medallion-layer-3": "gold"
}

# Print variables and their values
for key, value in config.items():
    print(f"{key}: {value}")

# Convert the config dictionary to a JSON string for exit
config_json = json.dumps(config)

# Return the config dictionary as a JSON string when this notebook is run
dbutils.notebook.exit(config_json)

