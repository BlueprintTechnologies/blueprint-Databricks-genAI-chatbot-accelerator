# Databricks notebook source
# MAGIC %md
# MAGIC # What you will accomplish with this notebook
# MAGIC This notebook deletes all the artifacts created by this accelerator.
# MAGIC
# MAGIC 1. Vector Search: Delete the vector search index that was created in the `03-Unity-Catalog-silver-layer` notebook
# MAGIC 2. Vector Search: Delete the vector search endpoint that was created in the `03-Unity-Catalog-silver-layer` notebook
# MAGIC 3. Unity Catalog: Delete the entire catalog create in `02-Unity-Catalog-bronze-layer`, `03-Unity-Catalog-silver-layer`, and `04-GenAI-Goodness` notebooks

# COMMAND ----------

# MAGIC %pip install -q databricks-vectorsearch

# COMMAND ----------

# Restart the kernel to use updated packages
dbutils.library.restartPython()

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

catalog_name = config['catalog']
schema_name = config['schema']
silver_layer_name = config['medallion-layer-2']
volume_name = config['volume']

# COMMAND ----------

# DELETE THE SEARCH INDEX
from databricks.vector_search.client import VectorSearchClient

# Initialize the Vector Search Client
vectorClient = VectorSearchClient()

# Set the vector search endpoint name
vector_search_endpoint_name = "employee_handbook_vector_search_endpoint"

# Set the vector search index
vs_index_fullname = f"{catalog_name}.{schema_name}.{silver_layer_name}_{volume_name}_vector_search_index"

# Delete the vector search index
vectorClient.delete_index(
    endpoint_name=vector_search_endpoint_name,
    index_name=vs_index_fullname
)

# COMMAND ----------

# DELETE THE VECTOR SEARCH ENDPOINT
vectorClient.delete_endpoint(vector_search_endpoint_name)

# COMMAND ----------

# Delete the catalog in Unity Catalog we created, according to https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-drop-catalog.html

# Check if the catalog exists
catalogs_list = spark.sql("SHOW CATALOGS")
catalog_exists = catalog_name in [row[0] for row in catalogs_list.collect()]

if catalog_exists:
    try:
        # Let's get rid of the entire catalog we created
        spark.sql(f"""DROP CATALOG IF EXISTS {catalog_name} CASCADE;""")
        print(f"Successfully deleted the {catalog_name} catalog and all its contents")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
else:
    print(f"The catalog {catalog_name} was not found.")
