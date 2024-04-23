# Databricks notebook source
# MAGIC %md
# MAGIC ## Aquí se van a crear las carpetas necesarias para landing y departing

# COMMAND ----------

# MAGIC %md
# MAGIC ### Landing
# MAGIC Se van a crear 4 carpetas que identifican la división funcional:
# MAGIC - films
# MAGIC - rentals
# MAGIC - customers
# MAGIC - stores
# MAGIC
# MAGIC Dentro de cada una de ellas se va a crear una carpeta donde se dejarán cada uno de los ficheros de origen

# COMMAND ----------

# Definición de variables
absoluth_path = 'abfss://landing@externalstoragedvdrent.dfs.core.windows.net/'
base_folders = ['films', 'rentals', 'customers', 'stores']
films = ['film', 'film_category', 'language', 'film_actor', 'category', 'actor']
rentals = ['rental', 'payment']
customers = ['customer']
stores = ['store', 'staff', 'country', 'city', 'address', 'inventory']

# COMMAND ----------

# Creación de carpetas
for base_folder in base_folders:
    for folder in eval(base_folder):
        dbutils.fs.mkdirs(absoluth_path + base_folder + '/' + folder)
