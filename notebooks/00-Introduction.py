# Databricks notebook source
# MAGIC %md
# MAGIC # The Use Case
# MAGIC Generative AI unlocks incredible value from data. The key is starting with a solid business use case to truly realize that value.
# MAGIC
# MAGIC The use case we will apply this accelerator is from our HR department. In fact, the exact ask was "Imagine if we flipped our HR inbox/etc. into something similar like a bot…trained it with our handbook, IT policies, benefits, engagement protocol, etc…it would cut down on so many of the asks!"
# MAGIC
# MAGIC To demonstrate this use case, this accelerator starts with raw data data in the form of a publicly available employee handbook. We use Valve's New Employee Handbook, which aside from being a fun read, is publicly available with a full path to the [PDF](https://cdn.cloudflare.steamstatic.com/apps/valve/Valve_NewEmployeeHandbook.pdf).

# COMMAND ----------

# MAGIC %md
# MAGIC ## About This Accelerator
# MAGIC This accelerator is self-contained. 
# MAGIC 1. No third party services are used. Everything, including your data, stays within your Databricks workspace.
# MAGIC 2. Although your workspace must attached to a Unity Catalog metastore, this accelerator generates the catalog, schema, tables and volumes for you.
# MAGIC 2. This accelerator leverages a managed volume in Unity Catalog. External storage does not have to be defined.
# MAGIC 3. No cluster init scripts are used.
# MAGIC 4. All code is displayed in the notebooks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Prerequisites
# MAGIC - The workspace must be attached to a Unity Catalog metastore. For help on this, see https://docs.databricks.com/en/data-governance/unity-catalog/get-started.html.
# MAGIC - Serverless compute enabled.
# MAGIC - Personal access tokens must be enabled. For more information on this, see https://docs.databricks.com/en/administration-guide/access-control/tokens.html
# MAGIC - Access to Databricks Foundation Model APIs. For more information on this, see https://docs.databricks.com/en/machine-learning/foundation-models/index.html. 

# COMMAND ----------

# MAGIC %md
# MAGIC # Before You Get Started
# MAGIC ## Medallion Architecture
# MAGIC Although not required, it would be helpful to be familiar with the medallion architecture used in a lakehouse. The layers in the architecture are referred to as bronze, silver, and gold. However, your organization may utilize different terminology. You can learn more about the medallion architecture [here](https://www.databricks.com/glossary/medallion-architecture).
# MAGIC ![Image of medallion architecture](https://cms.databricks.com/sites/default/files/inline-images/building-data-pipelines-with-delta-lake-120823.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Recommended Compute
# MAGIC The Databricks workspace used to test this accelerator is in the West US 2 region. 
# MAGIC - Compute policy: `unrestricted`
# MAGIC - Databricks Runtime: `14.3 LTS ML`
# MAGIC - Worker type: `Standard_DS3_v2`
# MAGIC - Min workers: `1`
# MAGIC - Max workers: `2`
# MAGIC - Autoscaling: `yes`
# MAGIC - Photon acceleration: `no`
# MAGIC - Termination period: `10 minutes`

# COMMAND ----------

# MAGIC %md
# MAGIC # Optimization
# MAGIC During the course of building out this accelerator, we made heavy use of the [Lakehouse Optimizer (LHO)](https://azuremarketplace.microsoft.com/en/marketplace/apps/blueprint-consulting-services-llc.lakehouse-monitor?tab=overview) to analyze our compute performance and orchestration. 
# MAGIC
# MAGIC Early on, based on back-of-napkin estimates, we had configured our compute worker for a minimum of 4 and maximum of 8. However, after evaluating the `CPU Process Load` and `Process Memory Load` KPIs in LHO, we adjusted our compute without a noticeable impact to performance.
# MAGIC