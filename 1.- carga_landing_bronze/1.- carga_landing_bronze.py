# Databricks notebook source
# MAGIC %md
# MAGIC ### Importación de librerías

# COMMAND ----------

from datetime import date

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creación de la función que mueve el fichero de landing a bronze

# COMMAND ----------

def landing_to_bronze(folder, file):
    date_data = date.today()
    year = date_data.strftime('%Y')
    month = date_data.strftime('%m')
    day = date_data.strftime('%d')
    source_path = 'abfss://landing@externalstoragedvdrent.dfs.core.windows.net/' + folder + '/' + file + '/' + file +'.csv'
    destination_path = 'abfss://datalake@metastoredgp01.dfs.core.windows.net/bronze/' + folder + '/' + file + '/' + year + '/' + \
        month + '/' + day + '/' + file + '_' + year + month + day + '.csv'
    dbutils.fs.mv(source_path, destination_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Invocación a la función con los parámetros elegidos

# COMMAND ----------

folder = dbutils.widgets.get('folder')
file = dbutils.widgets.get('file')
landing_to_bronze(folder, file)
