# Databricks notebook source
# MAGIC %md
# MAGIC ### Importación de librerías

# COMMAND ----------

from datetime import date, datetime
from pyspark.sql.functions import sha2, lit, concat
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ###Definición de funciones

# COMMAND ----------

def insert_hub(df_file, hub_table, pk, columns, hub_table_name):
    df_insert = df_file.join(hub_table, pk, 'leftanti')
    df_insert = df_insert.dropDuplicates([pk])
    df_insert = df_insert.select(columns)

    if df_insert.count() > 0:
        df_insert.write \
            .mode('append') \
            .saveAsTable('silver.' + folder + '.' + hub_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definición de variables

# COMMAND ----------

folder = 'films'
file = 'actor'

date_data = date.today()
year = date_data.strftime('%Y')
month = date_data.strftime('%m')
day = date_data.strftime('%d')

file_name = file + '_' + year + month + day + '.csv'

source_file = 'abfss://datalake@metastoredgp01.dfs.core.windows.net/bronze/' + folder + '/' + file + '/' + year + '/' + \
    month + '/' + day + '/' + file_name

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga de fichero

# COMMAND ----------

df = spark.read \
    .option("header","true") \
    .option("delimiter","|") \
    .option("inferSchema", "true") \
    .csv(source_file)

df = df.withColumn('actor_key', sha2(df.actor_id.cast(StringType()), 256))
df = df.withColumn('rec_src', lit(file_name))
df = df.withColumn('load_date', lit(datetime.now()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga de las tablas destino

# COMMAND ----------

hub_actor = spark.read.table('silver.' + folder + '.hub_actor')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Selección de registros que no están en la tabla de destino e inserto en tabla hub_actor

# COMMAND ----------

df_file = df
hub_table = hub_actor
pk = 'actor_key'
columns = ['actor_key', 'first_name', 'last_name', 'rec_src', 'load_date']
hub_table_name = 'hub_actor'

insert_hub(df_file, hub_table, pk, columns, hub_table_name)
