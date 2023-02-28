# Databricks notebook source
!pip install --upgrade pip
!pip install databricks-cli

# COMMAND ----------

!pip install databricks-cli --upgrade

# COMMAND ----------

!databricks --version

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

!echo "[DEFAULT] host = $url token = $token" > ~/.databricks.cfg

# COMMAND ----------

!cat ~/.databricks.cfg

# COMMAND ----------



# COMMAND ----------

 !(echo -e 'x\ny')

# COMMAND ----------

!databricks runs list

# COMMAND ----------

!databricks jobs list

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

!apt-get install -y jq

# COMMAND ----------

!curl --netrc --request POST https://tfs-prod.cloud.databricks.com/api/2.0/token/create --data '{ "comment": "This is an example token", "lifetime_seconds": 7776000 }' | jq .

# COMMAND ----------

import requests


# COMMAND ----------



# Get a token from https://<your-databricks-instance>/#secrets/createScope
# token = dbutils.secrets.get(scope = "<some-scope>", key = "<token>")
token=''

# Get the list of jobs
jobs = requests.get(
    "https://tfs-prod.cloud.databricks.com/api/2.1/jobs/list",
    headers = {
        "Authorization": "Bearer " + token
    }
)

# Get the list of runs
runs = requests.get(
    "https://tfs-prod.cloud.databricks.com/api/2.1/jobs/runs/list",
    headers = {
        "Authorization": "Bearer " + token
    }
)

# COMMAND ----------

runs

# COMMAND ----------


