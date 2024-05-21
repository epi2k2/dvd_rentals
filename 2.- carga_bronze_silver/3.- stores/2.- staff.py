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

folder = 'stores'
file = 'staff'

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

df = df.withColumn('staff_key', sha2(df.staff_id.cast(StringType()), 256))
df = df.withColumn('store_key', sha2(df.store_id.cast(StringType()), 256))
df = df.withColumn('address_key', sha2(df.address_id.cast(StringType()), 256))
df = df.withColumn('rec_src', lit(file_name))
df = df.withColumn('load_date', lit(datetime.now()))
df = df.withColumn('start_date', df.load_date)
df = df.withColumn('hash_diff', sha2(concat(df.address_id.cast(StringType()), \
    df.email.cast(StringType()), \
        df.store_id.cast(StringType()), \
            df.active.cast(StringType()), \
                df.username.cast(StringType()), \
                    df.password.cast(StringType()), \
                        df.picture.cast(StringType()) \
                            ), 256))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga de las tablas destino

# COMMAND ----------

hub_staff = spark.read.table('silver.' + folder + '.hub_staff')
sat_staff = spark.read.table('silver.' + folder + '.sat_staff')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Selección de registros que no están tabla hub_staff

# COMMAND ----------

df_file = df
hub_table = hub_staff
pk = 'staff_key'
columns = ['staff_key', 'first_name', 'last_name', 'rec_src', 'load_date']
hub_table_name = 'hub_staff'

insert_hub(df_file, hub_table, pk, columns, hub_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Selección de registros que no están en la tabla sat_staff

# COMMAND ----------

df_file = df
sat_table = sat_staff
pk = 'staff_key'
columns = ['staff_key', 'address_key', 'email', 'active', \
  'username', 'password', 'picture', 'hash_diff', \
    'rec_src', 'start_date']
sat_table_name = 'sat_staff'

insert_sat(df_file, sat_table, pk, columns, sat_table_name)
