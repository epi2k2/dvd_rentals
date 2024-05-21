# Databricks notebook source
# MAGIC %md
# MAGIC ### Importación de librerías

# COMMAND ----------

from datetime import date
import time

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creación de la función que crea el fichero en departing

# COMMAND ----------

def gold_to_departing(schema, table):
    date_data = date.today()
    year = date_data.strftime('%Y')
    month = date_data.strftime('%m')
    day = date_data.strftime('%d')
    external_location = 'abfss://departing@externalstoragedvdrent.dfs.core.windows.net/'
    destination_path = external_location + schema + '/' + table + '/' + year + '/' + \
        month + '/' + day + '/' + table + '_' + year + month + day + '.csv'
    delta_df = spark.table('gold.' + schema + '.' + table)
    delta_df.write \
        .format('csv') \
            .mode('overwrite') \
                .option("header", "true") \
                    .save(destination_path)
    
    for file in dbutils.fs.ls(destination_path):
        if file.name.endswith('.csv'):
            display(destination_path + '/' + file.name)
            dbutils.fs.mv(destination_path + '/' + file.name, external_location + schema + '/' + table + '/' + year + '/' + \
                month + '/' + day + '/' +  file.name)
            dbutils.fs.rm(destination_path, recurse=True)
            dbutils.fs.mv(external_location + schema + '/' + table + '/' + year + '/' + \
                month + '/' + day + '/' +  file.name, external_location + schema + '/' + table + '/' + year + '/' + \
                month + '/' + day + '/' +  table + '_' + year + month + day + '.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creación del fichero

# COMMAND ----------

schema = dbutils.widgets.get('schema')
table = dbutils.widgets.get('table')
gold_to_departing(schema, table)
