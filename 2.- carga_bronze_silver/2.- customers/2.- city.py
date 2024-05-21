# Databricks notebook source
# MAGIC %md
# MAGIC ### Importación de librerías

# COMMAND ----------

from datetime import date, datetime
from pyspark.sql.functions import sha2, lit, concat
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


def insert_link(df_file, link_table, pk, columns, link_table_name):
    df_insert = df_file.join(link_table, pk, 'leftanti')
    df_insert = df_insert.select(columns)

    if df_insert.count() > 0:
        df_insert.write \
            .mode('append') \
            .saveAsTable('silver.' + folder + '.' + link_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definición de variables

# COMMAND ----------

folder = 'customers'
file = 'city'

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

df = df.withColumn('city_key', sha2(df.city_id.cast(StringType()), 256))
df = df.withColumn('country_key', sha2(df.country_id.cast(StringType()), 256))
df = df.withColumn('country__city_key', sha2(concat(df.country_id.cast(StringType()), \
    df.city_id.cast(StringType())), 256))
df = df.withColumn('rec_src', lit(file_name))
df = df.withColumn('load_date', lit(datetime.now()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga de la tabla destino

# COMMAND ----------

hub_city = spark.read.table('silver.' + folder + '.hub_city')
link_country__city = spark.read.table('silver.' + folder + '.link_country__city')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Selección de registros que no están en la tabla de destino e inserto en tabla hub_city

# COMMAND ----------

df_file = df
hub_table = hub_city
pk = 'city_key'
columns = ['city_key', 'city', 'country_id', 'rec_src', 'load_date']
hub_table_name = 'hub_city'

insert_hub(df_file, hub_table, pk, columns, hub_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Selección de registros que no están en la tabla link_country__city

# COMMAND ----------

df_file = df
link_table = link_country__city
pk = 'city_key'
columns = ['country__city_key', 'city_key', 'country_key', 'rec_src', 'load_date']
link_table_name = 'link_country__city'

insert_link(df_file, link_table, pk, columns, link_table_name)
