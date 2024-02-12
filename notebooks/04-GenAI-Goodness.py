# Databricks notebook source
# MAGIC %md
# MAGIC # What you will accomplish with this notebook
# MAGIC We are building a chat bot in this notebook. This notebook leverages a variety of Databricks services and other tools.
# MAGIC
# MAGIC 1. Databricks Vector Search: Provides the vector data to the language model
# MAGIC 2. Foundation Model: We are going to use the `databricks-llama-2-70b-chat` model
# MAGIC 3. LangChain: Model wrapping, prompt template, vector retrieval, and chaining everything together
# MAGIC 4. Delta Lake: Storing a new dataset from the questions and answers that people ask of the chatbot
# MAGIC
# MAGIC **IMPORTANT**: This notebook relies on the Delta Lake, Unity Catalog, and Vector Search setup completed by the other notebooks in this accelerator.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Use Case

# COMMAND ----------

# MAGIC %pip install -q databricks-vectorsearch langchain_community

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

from langchain_community.vectorstores import DatabricksVectorSearch
from databricks.vector_search.client import VectorSearchClient

vs_catalog = config['catalog']
vs_schema = config['schema']
vs_volume = config['volume']
vs_layer_one = config['medallion-layer-1']
vs_layer_two = config['medallion-layer-2']

#We build the endpoint and index name from the values defined in the 01-Configuration notebook. 
vector_search_endpoint_name = vs_schema + "_vector_search_endpoint"
vector_search_index_name = vs_catalog + "." + vs_schema + "." + vs_layer_two + "_" + vs_volume + "_vector_search_index"

# Initialize the Vector Search Client
vs_client = VectorSearchClient(disable_notice=True)
vs_index = vs_client.get_index(
    endpoint_name=vector_search_endpoint_name,
    index_name=vector_search_index_name
)

vectorstore = DatabricksVectorSearch(vs_index)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's test our Vector Search endpoint & index

# COMMAND ----------

question = "What is this document?"

vectorstore.similarity_search(question)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Language Model
# MAGIC We use LangChain to wrap a Databricks foundation model endpoint. We use the `databricks-llama-2-70b-chat` model in this accelerator.
# MAGIC
# MAGIC Learn more about the ChatDatabricks class at https://api.python.langchain.com/en/latest/chat_models/langchain_community.chat_models.databricks.ChatDatabricks.html

# COMMAND ----------

from langchain.chat_models import ChatDatabricks

chat_model = ChatDatabricks(endpoint="databricks-llama-2-70b-chat", max_tokens = 200)

#Test the model
print(f"Response: {chat_model.predict('What are you?')}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## The Data Retrieval
# MAGIC Now we build a function to retrieve relevant data from our vector database in Vector Search.

# COMMAND ----------

# Let's get our vector search in the chain as a retriever
def get_retriever(persist_dir: str = None):
    
    # Initialize the Vector Search Client
    vs_client = VectorSearchClient(disable_notice=True)
    vs_index = vs_client.get_index(
        endpoint_name=vector_search_endpoint_name,
        index_name=vector_search_index_name
    )

    # Create the retriever
    vectorstore = DatabricksVectorSearch(
        vs_index
    )
    
    # To avoid reaching 'max content length is 4097 tokens', reducing the similarity results to 2
    return vectorstore.as_retriever(search_kwargs={"k": 2})

# COMMAND ----------

# MAGIC %md 
# MAGIC ## The Prompt
# MAGIC Now we craft our prompt for the language model using LangChain's PromptTemplate class. You can learn more about LangChain promp templates [here](https://api.python.langchain.com/en/latest/prompts/langchain_core.prompts.prompt.PromptTemplate.html#).

# COMMAND ----------

#Now we build our chat app chain in LanChain

from langchain.chains import RetrievalQA
from langchain.prompts import PromptTemplate

TEMPLATE = """You are an assistant for Human Resources. You are answering questions from new employees with content found in the new employee handbook. This handbook is about the choices they will be making and how to think about them. If the question is not related to this topic, kindly decline to answer. If you don't know the answer, just say that you don't know, don't try to make up an answer. Keep the answer as concise as possible.
Use the following pieces of context to answer the question at the end:
{context}
Question: {question}
Answer:
"""

prompt = PromptTemplate(template=TEMPLATE, input_variables=["context", "question"])



# COMMAND ----------

# MAGIC %md
# MAGIC ## Putting it all together
# MAGIC Now we use LangChain to put all the pieces together.

# COMMAND ----------

chain = RetrievalQA.from_chain_type(
    llm=chat_model,
    chain_type="stuff",
    retriever=get_retriever(),
    chain_type_kwargs={"prompt": prompt}
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare for saving questions & answers
# MAGIC Let's get ready to save the questions and answers from the chatbot to a bronze Delta table.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession, Row
from datetime import datetime


# Define the schema
chatSchema = StructType([
    StructField("question", StringType(), True),
    StructField("answer", StringType(), True),
    StructField("timestamp", StringType(), True)
])

#We build the table name from the values defined in the 01-Configuration notebook
bronze_qa_table = vs_catalog + "." + vs_schema + "." + vs_layer_one + "_" + vs_volume + "_chat_responses"
    
def ensure_delta_table_exists(schema):
    
    if not spark._jsparkSession.catalog().tableExists(bronze_qa_table):
        # The table does not exist, create an empty DataFrame with the defined schema and save it as a Delta table
        empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
        empty_df.write.format("delta").saveAsTable(bronze_qa_table)

def write_to_delta(question, answer):
    
    ensure_delta_table_exists(chatSchema)
    
    # Use Python's datetime to generate a current timestamp as a string
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Create a DataFrame directly with a list of Rows, including the timestamp
    new_data = spark.createDataFrame([
        Row(question=question, answer=answer, timestamp=current_time)
    ], chatSchema)
    
    try:   
        # Append the new data to the Delta table
        new_data.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(bronze_qa_table)
        print("Data written successfully.")
    except Exception as e:
        print(f"Failed to write data: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Let's build an interface
# MAGIC To add a bit of spice to the interaction, let's create a chatbot interface directly in the notebook using iPyWidgets. We've already setup all the logic and functionality previous, so we can focus on the layout.

# COMMAND ----------

# DBTITLE 1,The Chat Bot
import ipywidgets as widgets
from IPython.display import display, Markdown
from IPython.display import HTML
from ipywidgets import widgets, HTML, Layout, ButtonStyle, HBox

# Create a button widget for generating the answer
button = widgets.Button(
    description='Ask',
    layout=Layout(width='10%', height='40px'),  # Set the width and height
    button_style='success',  # 'success', 'info', 'warning', 'danger' or ''
    style=ButtonStyle(button_color='#1e30c7')  # Set the button color
)

# introduction text field
introTextHTML = HTML(
    value="<div style='font-size: 20px;'>Valve works in ways that might seem counterintuitive at first.<br>This chatbot provides a question and answer interaction to the publicly available New Employee Handbook.</div>",
    layout=Layout(width='auto')
)

# Create a text widget for input
questionText = widgets.Textarea(
    value='',
    placeholder='Ask a question that will help you not freak out at work...',
    description='',
    layout=Layout(width='90%', height='40px'),
    style={'description_width': 'initial'}
)

answerText = widgets.Textarea(
    value='',
    placeholder='The answer you seek is here...',
    description='',
    layout=Layout(width='100%', height='200px'),
    style={'description_width': 'initial'}
)

# Create a horizontal box to hold the text and button widgets
box1 = HBox([introTextHTML])
box2 = HBox([questionText, button])
box3 = HBox([answerText])

# Display the box
display(box1, box2, box3)

def on_button_clicked(b):
    answerText.value = "Ok, got your question. Give me a few moments while I squeeze the lemons..."
    
    question = {"query": questionText.value}
    answer = chain.run(question)
    answerText.value = answer

    write_to_delta(questionText.value, answer)

button.on_click(on_button_clicked)

# COMMAND ----------

# MAGIC %md
# MAGIC # What has been accomplished in this notebook?
# MAGIC
# MAGIC A fully functional GenAI application!
# MAGIC
# MAGIC A chat bot interface, built using iPyWidgets, that leverages the databricks-llama-2-70b-chat Foundation Model serving endpoint. If you look in the Serving explorer, you will see the databricks-llama-2-70b-chat endpoint.
# MAGIC
# MAGIC A new bronze table created in your lakehouse. If you look in the Catalog explorer, you will see:
# MAGIC - One new Delta table in the previously created schema that houses the questions and answers from the chat bot. By default, we use "bronze_hr_docs_chat_responses".