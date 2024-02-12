# Databricks notebook source
# MAGIC
# MAGIC %md
# MAGIC # What you will accomplish with this notebook
# MAGIC This notebook leverages a variety of Databricks services and APIs.
# MAGIC
# MAGIC 1. Workflows: Utilize a configuration notebook and pass variables between notebooks using a notebook job
# MAGIC 2. Unity Catalog: Create a new catalog and schema
# MAGIC 3. Unity Catalog: Create a managed volume to store documents
# MAGIC 4. Unity Catalog: Download a publicly available PDF and store in a managed volume
# MAGIC 5. AutoLoader: Load raw PDF from managed volume into a new Delta table
# MAGIC 6. Delta Lake: Create the first level of a medallion architecture with a bronze table

# COMMAND ----------

# Let's do our imports
from datetime import datetime

# COMMAND ----------

# Lets pull in our configuration info from the 01-Configuration notebook
import json

# Run the configuration notebook and capture its JSON output
config_json = dbutils.notebook.run("./01-Configuration", timeout_seconds=60)

# Parse the JSON string back into a Python dictionary
config = json.loads(config_json)

# Configuration values are accessible via a config['key'] interface

print(config)  # Print the dictionary of configurations for reference

# COMMAND ----------

#create a new catalog in Unity Catalog
catalog_name = config['catalog']

# Attempt to create the catalog and handle exceptions
try:
    # Execute the Spark SQL command to create the catalog
    spark.sql(f"""CREATE CATALOG IF NOT EXISTS {catalog_name}
              COMMENT 'A new catalog for the {catalog_name} use case'""")
    
    # Fetch the current date and time
    completion_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # If the command is executed successfully, print a custom success message
    print(f"The catalog '{catalog_name}' was created successfully on {completion_time}.")
except Exception as e:
    # In case of an exception, print the error message
    print(f"An error occurred: {e}")


# COMMAND ----------

schema_name = config['schema']

try:
    # Set the current catalog context
    spark.sql(f"""USE CATALOG {catalog_name}""")
    
    # Execute the Spark SQL command to create the schema
    spark.sql(f"""
              CREATE SCHEMA IF NOT EXISTS {schema_name}
              COMMENT 'A new schema for the {catalog_name} catalog.'""")
              
    # Fetch the current date and time
    completion_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # If the commands execute successfully, print a custom success message
    print(f"The schema '{schema_name}' was created successfully in the catalog '{catalog_name}' on {completion_time}.")
except Exception as e:
    # In case of an exception, print the error message
    print(f"An error occurred: {e}")


# COMMAND ----------

# Create a managed volume in the catalog per https://docs.databricks.com/en/connect/unity-catalog/volumes.html
volume_name = config['volume']

try: 
    # Create the volume if it doesn't exist 
    spark.sql(f"""CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}""")

    # Fetch the current date and time
    completion_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # If the commands execute successfully, print a custom success message
    print(f"The volume '{volume_name}' was created successfully in the schema '{schema_name}' on {completion_time}.")
except Exception as e:
    # In case of an exception, print the error message
    print(f"An error occurred: {e}")

# COMMAND ----------

# Let's grab the employee handbook from the internet and store in the Unity Catalog managed volume

# Download the employee handbook from a public location - https://docs.databricks.com/en/files/download-internet-files.html#language-python
import urllib
urllib.request.urlretrieve("https://cdn.cloudflare.steamstatic.com/apps/valve/Valve_NewEmployeeHandbook.pdf", "/tmp/Valve_NewEmployeeHandbook.pdf")

# Move it from tmp to the managed volume
managed_volume = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
dbutils.fs.mv("file:/tmp/Valve_NewEmployeeHandbook.pdf", managed_volume)

# Show it in the volume
display(dbutils.fs.ls(managed_volume))

# COMMAND ----------

from pyspark.sql.utils import StreamingQueryException

# Delta table time!

# Let's use autoloader to ingest the pdf
raw_layer_name = config['medallion-layer-1']

df = (spark.readStream
      .format('cloudFiles')
      .option('cloudFiles.format', 'BINARYFILE')
      .option('pathGlobFilter', "*.pdf")
      .load('dbfs:' + managed_volume)
      )

# Now let's set the table name
# NOTE: Include the schema so that the table does not get written to the default schema in the catalog
table_name = f"{schema_name}.{raw_layer_name}_{volume_name}_raw"

# Now let's get the pdf into a delta table
try:
    query = (df.writeStream
             .trigger(availableNow=True)
             .option("checkpointLocation", f'dbfs:{managed_volume}/checkpoints/{table_name}')
             .table(table_name)
             .awaitTermination())
    
    print(f"Successfully ingested data and created the Delta table: {table_name}")

except StreamingQueryException as e:
    print(f"An error occurred during stream processing: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")


# COMMAND ----------

# Let's check out the contents of our Delta table
df = spark.sql(f"""SELECT * FROM {table_name}""")

# Print a message indicating successful retrieval
print(f"Successfully retrieved the table listing for: {table_name}")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # What has been accomplished in this notebook?
# MAGIC
# MAGIC If you look in the Catalog explorer, you will see:
# MAGIC 1. One new catalog in Unity Catalog, named after the value defined in the configuration notebook. By default, we use "hr_documentation".
# MAGIC 2. One new schema in that catalog, named after the value defined in the configuration notebook. By default, we use "employee handbook".
# MAGIC 3. One new volume in the new schema, named after the value defined in the configuration notebook. By default, we use "hr_docs".
# MAGIC      - Inside that volume you will see a pdf that was uploaded from a publicly accessible location.
# MAGIC      - Inside that volume you will see a checkpoints folder
# MAGIC 4. One new Delta table in the new schema, named with combination of values defined in the configuration notebook. By default, we use "bronze_hr_docs_raw"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC