# Databricks notebook source
# MAGIC %md
# MAGIC # What you will accomplish with this notebook
# MAGIC 1. External library usage: Extract text from the PDF binary content in the bronze table, using the fitz module from [PyMuPDF](https://pymupdf.readthedocs.io/en/latest/), a high-performance Python library for data extraction, analysis, conversion & manipulation of PDF (and other) documents.
# MAGIC 2. Delta Lake: Store extracted text in a new Delta table.
# MAGIC 3. Delta Lake: Chunk extracted data using a Spark native approach and store chunks in a new Delta table.
# MAGIC 4. Vector Search: Create a new endpoint
# MAGIC 5. Vector Search: Create the vector embeddings with Databricks' `databricks-bge-large-en` embedding model
# MAGIC 5. Vector Search: Create a new index
# MAGIC 6. Delta Sync: Set up the index to automatically sync with a Delta table
# MAGIC
# MAGIC # A few considerations
# MAGIC
# MAGIC **Administrator access required:** We are using the PyMuPDF library for text extraction from PDFs. For educational purposes, we are installing it from the notebook rather than a cluster configuration. Unfortunately, directly installing external libraries on Databricks workers can sometimes be challenging due to environment restrictions or the need for cluster configuration changes that only administrators can perform.
# MAGIC
# MAGIC **Dev environment choices:** For the vector search setup, we use a notebook authentication token. This is recommended for development only. For improved performance, use Service Principal based authentication.
# MAGIC
# MAGIC **Performance:** Extracting text from PDFs can be resource-intensive, and doing so within a UDF may not be the most efficient approach for large datasets. In this accelerator, we are only dealing with a single pdf. Test and monitor the performance for your specific use case.

# COMMAND ----------

# MAGIC %pip install -q PyMuPDF databricks-vectorsearch
# MAGIC

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

# Let's set some variables from our imported config
catalog_name = config['catalog']
schema_name = config['schema']
volume_name = config['volume']
raw_layer_name = config['medallion-layer-1']
bronze_table_name = f"{catalog_name}.{schema_name}.{raw_layer_name}_{volume_name}_raw"

# COMMAND ----------

# MAGIC %md
# MAGIC # Extract text from PDF

# COMMAND ----------

# Let's set up our PDF text extraction

from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

import fitz  # PyMuPDF

def extract_text_from_pdf(pdf_bytes):
    text = ""
    try:
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            for page in doc:
                text += page.get_text()
    except Exception as e:
        text = f"Error extracting text: {str(e)}"
    return text

# Register the UDF
extract_text_udf = udf(extract_text_from_pdf, StringType())



# COMMAND ----------

from pyspark.sql.functions import udf, explode, array, monotonically_increasing_id
# Create our extracted text silver table

# Get the relevant bronze table from Unity Catalog
pdf_df = spark.table(bronze_table_name)

# Define the silver table name
silver_layer_name = config['medallion-layer-2']
silver_table_extracted_text = f"{catalog_name}.{schema_name}.{silver_layer_name}_{volume_name}_extracted_text"

text_df = None #Initialize variable to handle it outside the try block

# Apply the UDF to our DataFrame
try:
    text_df = pdf_df.withColumn("extracted_text", extract_text_udf(col("content")))
except Exception as e:
    print(f"Failed to extract text from PDFs: {e}")

if text_df:
    try:
        text_df.write.format("delta").mode("overwrite").saveAsTable(silver_table_extracted_text)
        print(f"Successfully written extracted text to {silver_table_extracted_text}")
    except Exception as e:
        print(f"Failed to write to silver table {silver_table_extracted_text}: {e}")
    else:
        # Add a comment only if the write succeeds
        try:
            table_comment = "Contains extracted text from PDF documents in the bronze layer."
            alter_table_sql = f"""
            ALTER TABLE {silver_table_extracted_text}
            SET TBLPROPERTIES ('comment' = '{table_comment}')
            """
            spark.sql(alter_table_sql)
        except Exception as e:
            print(f"Failed to add comment to {silver_table_extracted_text}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Time for the chunking

# COMMAND ----------


from pyspark.sql.types import StringType, ArrayType

# Assuming an average token length for estimation. Adjust based on your data.
# This is a simplistic approach; consider word boundaries for a more accurate implementation.
avg_token_length = 6
max_chunk_size = 512 * avg_token_length

def chunk_text(text, chunk_size=max_chunk_size):
    return [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]

chunk_text_udf = udf(chunk_text, ArrayType(StringType()))


# COMMAND ----------

# Ensure text_df is not None
if text_df:
    # Apply the chunking UDF and explode the array of chunks into separate rows
    chunked_text_df = text_df.withColumn("text_chunks", explode(chunk_text_udf("extracted_text")))

    # Add an identifier for each chunk
    chunked_text_df = chunked_text_df.withColumn("chunk_id", monotonically_increasing_id())

    # Because we used Auto Loader with cloudFiles format in the bronze layer, using the path as a document id
    final_chunked_df = chunked_text_df.selectExpr("path as document_id", "chunk_id", "text_chunks as chunked_text")


# COMMAND ----------

# Define a new silver table name for chunked text
silver_chunked_table_name = f"{silver_table_extracted_text}_chunks"

# Write a DataFrame with chunked text to the silver table. Change Data Feed must be enabled for use by vector search.
try:
    final_chunked_df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .option("delta.enableChangeDataFeed", "true") \
        .saveAsTable(silver_chunked_table_name)
    print(f"Successfully written chunked text to {silver_chunked_table_name}")
except Exception as e:
    print(f"Failed to write to silver table {silver_chunked_table_name}: {e}")


# COMMAND ----------

# Add a comment to the newly created silver table for chunked text
table_comment = "Contains chunked text from silver extracted text, ready for embedding analysis."
alter_table_sql = f"""
ALTER TABLE {silver_chunked_table_name}
SET TBLPROPERTIES ('comment' = '{table_comment}')
"""
try:
    spark.sql(alter_table_sql)
except Exception as e:
    print(f"Failed to add comment to {silver_chunked_table_name}: {e}")


# COMMAND ----------

# MAGIC %md 
# MAGIC # Create Vector Search Endpoint
# MAGIC Note: This feature is in Public Preview in the us-east-1, us-east-2, us-west-2, eu-west-1, and ap-southeast-2 regions. Learn more [here](https://docs.databricks.com/en/generative-ai/vector-search.html).

# COMMAND ----------

import time

# Got this from a Databricks example
def is_endpoint_ready(vsc, vs_endpoint_name):
  for i in range(180):
    try:
      endpoint = vsc.get_endpoint(vs_endpoint_name)
    except Exception as e:
      if "REQUEST_LIMIT_EXCEEDED" in str(e):
        print("WARN: couldn't get endpoint status due to REQUEST_LIMIT_EXCEEDED error. Please manually check your endpoint status")
        return
      else:
        raise e
    status = endpoint.get("endpoint_status", endpoint.get("status"))["state"].upper()
    if "ONLINE" in status:
      return endpoint
    elif "PROVISIONING" in status or i <6:
      if i % 20 == 0: 
        print(f"Waiting for endpoint to be ready. This can take between 5 to 10 minutes.")
        # {endpoint}
      time.sleep(10)
    else:
      raise Exception(f'''Error with the endpoint {vs_endpoint_name}.''')
  raise Exception(f"Timeout, your endpoint isn't ready yet: {vsc.get_endpoint(vs_endpoint_name)}")

# COMMAND ----------

# Now we create the vector search index
# Time to create: ~ 10 minutes

from databricks.vector_search.client import VectorSearchClient

# Initialize the Vector Search Client
vectorClient = VectorSearchClient()

# Define your vector search endpoint name
vector_search_endpoint_name = "employee_handbook_vector_search_endpoint"

# Check if the endpoint already exists
existing_endpoints = vectorClient.list_endpoints().get('endpoints', [])
if vector_search_endpoint_name not in [e['name'] for e in existing_endpoints]:
    # Create the endpoint if it does not exist
    vectorClient.create_endpoint(
        name=vector_search_endpoint_name,
        endpoint_type="STANDARD"
    )
    print(f"Endpoint {vector_search_endpoint_name} is being created...")

    # Wait for the endpoint to be ready
    is_endpoint_ready(vectorClient, vector_search_endpoint_name)
    print(f"Endpoint named {vector_search_endpoint_name} is ready for some goodness.")
else:
    print(f"Endpoint named {vector_search_endpoint_name} already exists and is ready for use.")


# COMMAND ----------

# MAGIC %md
# MAGIC # Create a Vector Search Index
# MAGIC With Vector Search, we have the option to provide our source Delta table and have the Vector Search service calculate the embeddings. For this accelerator, we are using the `databricks-bge-large-en` embedding model that Databricks provides.

# COMMAND ----------

# Vector index
vs_index = f"{silver_table_extracted_text}_chunks"

vs_index_fullname = f"{catalog_name}.{schema_name}.{silver_layer_name}_{volume_name}_vector_search_index"

embedding_model_endpoint = "databricks-bge-large-en"

# Got this from a Databricks example
def is_index_ready(vsc, vs_endpoint_name, index_name):
  for i in range(180):
    idx = vsc.get_index(vs_endpoint_name, index_name).describe()
    index_status = idx.get('status', idx.get('index_status', {}))
    status = index_status.get('detailed_state', index_status.get('status', 'UNKNOWN')).upper()
    url = index_status.get('index_url', index_status.get('url', 'UNKNOWN'))
    if "ONLINE" in status:
      return
    if "UNKNOWN" in status:
      print(f"Can't get the status - will assume index is ready {idx} - url: {url}")
      return
    elif "PROVISIONING" in status:
      if i % 40 == 0: print(f"Waiting for index to be ready, this can take a few min... {index_status} - pipeline url:{url}")
      time.sleep(10)
    else:
        raise Exception(f'''Error with the index - this shouldn't happen. DLT pipeline might have been killed.\n Please delete it and re-run the previous cell: vsc.delete_index("{index_name}, {vs_endpoint_name}") \nIndex details: {idx}''')
  
  raise Exception(f"Timeout, your index isn't ready yet: {vsc.get_index(index_name, vs_endpoint_name)}")

# COMMAND ----------

# Create a Delta Sync 
try: 
  # Attempt to create the Delta Sync index
  index = vectorClient.create_delta_sync_index(
    endpoint_name=vector_search_endpoint_name,
    source_table_name=f"{silver_table_extracted_text}_chunks",
    index_name=vs_index_fullname,
    pipeline_type='TRIGGERED',
    primary_key="chunk_id",
    embedding_source_column="chunked_text",
    embedding_model_endpoint_name=embedding_model_endpoint
  )
  index_status_message = index.describe()['status']['message']
  print(f"{index_status_message}")
except Exception as e:
    if "RESOURCE_ALREADY_EXISTS" in str(e):
        print(f"Index {vs_index_fullname} already exists. Skipping creation.")
    else:
        print(f"An error occurred: {str(e)}")

# COMMAND ----------

index = vectorClient.get_index(endpoint_name=vector_search_endpoint_name,index_name=vs_index_fullname)

while not index.describe().get('status')['ready']:
  print("Waiting for index to be ready...")
  time.sleep(30)
print("The vector search index is ready to go. Here is info about the index.")
index.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test the index

# COMMAND ----------

results = index.similarity_search(
  query_text="How can I be successful?",
  columns=["chunked_text"],
  num_results=5
  )
rows = results['result']['data_array']

for row in rows:
    chunk_text, score = row[0], row[1]  # Extract chunk_text and score from each row
    
    # Ensure chunk_text is a string (it should already be, based on your output)
    chunk_text = str(chunk_text) if chunk_text else ""

    # Remove line breaks from chunk_text
    chunk_text = chunk_text.replace("\n", " ")
    
    # Truncate chunk_text to 75 characters for display purposes
    if len(chunk_text) > 75:
        chunk_text = chunk_text[:75] + "..."
    
    # Note: There's no 'document_id' in the row based on the given output
    # If you need to associate each chunk with its original document, consider including document ID in your indexing or results fetching logic
    
    print(f"text: {chunk_text} score: {score}")

# COMMAND ----------

# MAGIC %md
# MAGIC # What has been accomplished in this notebook?
# MAGIC
# MAGIC This noteboook has created a silver layer in your lakehouse. If you look in the Catalog explorer, you will see:
# MAGIC 1. One new Delta table in the previously created schema that houses the text extracted from the pdf in the bronze table. By default, we use "silver_hr_docs_extracted_text".
# MAGIC 2. One new Delta table in the previously created schema that houses the results of chunking the pdf text extraction. By default, we use "silver_hr_docs_extracted_text_chunks".
# MAGIC 3. One new Delta table in the previously created schema that houses the vector search index. By default, we use "silver_hr_docs_vector_search_index".
# MAGIC
# MAGIC This notebook has created a new Vector Search endpoint. If you look in the Compute explorer:
# MAGIC 1. Under "Vector Search", you'll see an endpoint has been created. By default, we use "employee_handbook_vector_search_endpoint".
# MAGIC 2. Clicking into the endpoint, you'll see an index has been created. By default, we use "hr_documentation.employee_handbook.silver_hr_docs_vector_search_index"