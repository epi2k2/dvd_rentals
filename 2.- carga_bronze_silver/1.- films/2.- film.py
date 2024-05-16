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


def insert_link(df_file, link_table, pk, columns, link_table_name):
    df_insert = df_file.join(link_table, pk, 'leftanti')
    df_insert = df_insert.select(columns)

    if df_insert.count() > 0:
        df_insert.write \
            .mode('append') \
            .saveAsTable('silver.' + folder + '.' + link_table_name)

def insert_sat(df_file, sat_table, pk, columns, sat_table_name):
    df_insert = df_file.join(sat_table.where(sat_table.end_date.isNull()), [pk, 'hash_diff'], 'leftanti')
    df_insert = df_insert.select(columns)

    if df_insert.count() > 0:
        df_update_end_date = sat_table.join(df_insert, pk, 'inner')
        df_update_end_date = df_update_end_date.withColumn('end_date', lit(datetime.now()))
        df_update_end_date = df_update_end_date.select(sat_table_name + '.*').where(sat_table.end_date.isNull())
        df_update_end_date.write \
            .mode('overwrite') \
                .saveAsTable('silver.' + folder + '.' + sat_table_name)

        df_insert.write \
            .mode('append') \
            .saveAsTable('silver.' + folder + '.' + sat_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definición de variables

# COMMAND ----------

folder = 'films'
file = 'film'

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

df = df.withColumn('film_key', sha2(df.film_id.cast(StringType()), 256))
df = df.withColumn('language_key', sha2(df.language_id.cast(StringType()), 256))
df = df.withColumn('rating_key', sha2(df.rating.cast(StringType()), 256))
df = df.withColumn('rec_src', lit(file_name))
df = df.withColumn('load_date', lit(datetime.now()))
df = df.withColumn('start_date', df.load_date)
df = df.withColumn('hash_diff', sha2(concat(df.rental_duration.cast(StringType()), \
    df.rental_rate.cast(StringType()), \
        df.replacement_cost.cast(StringType()), \
            df.rating_key.cast(StringType()), \
                df.special_features.cast(StringType()), \
                    df.fulltext.cast(StringType())) \
                        , 256))
df = df.withColumnRenamed('title', 'film')
df = df.withColumnRenamed('fulltext', 'full_text')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga de las tablas destino

# COMMAND ----------

link_film = spark.read.table('silver.' + folder + '.link_film')
sat_film = spark.read.table('silver.' + folder + '.sat_film')
hub_rating = spark.read.table('silver.' + folder + '.hub_rating')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Selección de registros que no están tabla link_film

# COMMAND ----------

df_file = df
link_table = link_film
pk = 'film_key'
columns = ['film_key', 'film', 'description', 'release_year', 'language_key', 'length', 'rec_src', 'load_date']
link_table_name = 'link_film'

insert_link(df_file, link_table, pk, columns, link_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Selección de registros que no están en la tabla sat_film

# COMMAND ----------

df_file = df
sat_table = sat_film
pk = 'film_key'
columns = ['film_key', 'rental_duration', 'rental_rate', 'replacement_cost', \
  'rating_key', 'special_features', 'full_text', 'hash_diff', 'rec_src', 'start_date']
sat_table_name = 'sat_film'

insert_sat(df_file, sat_table, pk, columns, sat_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Selección de registros que no están en la tabla de destino e inserto en tabla hub_rating

# COMMAND ----------

df_file = df
hub_table = hub_rating
pk = 'rating_key'
columns = ['rating_key', 'rating', 'rec_src', 'load_date']
hub_table_name = 'hub_rating'

insert_hub(df_file, hub_table, pk, columns, hub_table_name)
