-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Gold
-- MAGIC Aquí se va a crear la capa gold. Va a estar formada por 2 esquemas:
-- MAGIC
-- MAGIC - mercado
-- MAGIC - clientes
-- MAGIC
-- MAGIC Se va a seguir la metodología Data Vault, creando tablas FACT y DIM.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creación del catálogo

-- COMMAND ----------

create catalog if not exists gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creación de los esquemas

-- COMMAND ----------

create schema if not exists gold.mercado;
create schema if not exists gold.clientes;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creación de las tablas del esquema mercado

-- COMMAND ----------

/*Most rented movies*/
create table if not exists gold.mercado.most_rented_movies(
  category string,
  film string,
  rental_date timestamp,
  staff_first_name string,
  staff_last_name string,
  store_id int,
  manager_first_name string,
  manager_last_name string,
  load_date timestamp
) using delta;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creación de las tablas del esquema clientes

-- COMMAND ----------

/*VIP Clients*/
create table if not exists gold.clientes.vip_clients(
  first_name string,
  last_name string,
  amount_per_month double,
  date string,
  email string,
  country string,
  city string,
  address string,
  address2 string,
  postal_code int,
  load_date timestamp
) using delta;

-- COMMAND ----------

/*Subscribed and unsubscribed clients*/
create table if not exists gold.clientes.subscribed_unsubscribed_clients(
  first_name string,
  last_name string,
  active boolean,
  load_date timestamp
) using delta;
