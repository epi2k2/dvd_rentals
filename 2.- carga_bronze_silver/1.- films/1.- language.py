# Databricks notebook source
# MAGIC %md
# MAGIC ### Importación de librerías

# COMMAND ----------

from datetime import date, datetime
from pyspark.sql.functions import sha2, lit
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definición de funciones

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
file = 'language'

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

df = df.withColumn('language_key', sha2(df.language_id.cast(StringType()), 256))
df = df.withColumn('rec_src', lit(file_name))
df = df.withColumn('load_date', lit(datetime.now()))
df = df.withColumnRenamed('name', 'language')
df = df.select(['language_key', 'language', 'rec_src', 'load_date'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga de la tabla destino

# COMMAND ----------

hub_language = spark.read.table('silver.' + folder + '.hub_language')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Selección de registros que no están en la tabla de destino e inserto en tabla hub_language

# COMMAND ----------

df_file = df
hub_table = hub_language
pk = 'language_key'
columns = ['language_key', 'language', 'rec_src', 'load_date']
hub_table_name = 'hub_language'

insert_hub(df_file, hub_table, pk, columns, hub_table_name)
