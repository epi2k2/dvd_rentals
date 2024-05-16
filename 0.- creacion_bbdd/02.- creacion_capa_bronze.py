# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze
# MAGIC Se va a crear la estructura de la capa bronze, una carpeta con el nombre bronze, que contendrá 4 carpetas que identifican la división funcional:
# MAGIC
# MAGIC - films
# MAGIC - rentals
# MAGIC - customers
# MAGIC - stores
# MAGIC
# MAGIC Dentro de cada carpeta habrá una carpeta para cada tipo de fichero recibido en landing, y dentro se va a separar por carpetas que van a indentificar año, mes y día. Ejemplo:
# MAGIC - film
# MAGIC   - films
# MAGIC     - 2006
# MAGIC       - 02
# MAGIC         - 01

# COMMAND ----------

# Definición de variables
absoluth_path = 'abfss://datalake@metastoredgp01.dfs.core.windows.net/'
datalake_folder = 'bronze/'
base_folders = ['films', 'rentals', 'customers', 'stores']
films = ['film', 'film_category', 'language', 'film_actor', 'category', 'actor']
rentals = ['rental', 'payment']
customers = ['customer', 'country', 'city', 'address']
stores = ['store', 'staff',  'inventory']

# COMMAND ----------

for base_folder in base_folders:
    for folder in eval(base_folder):
        dbutils.fs.mkdirs(absoluth_path + datalake_folder + base_folder + '/' + folder)
