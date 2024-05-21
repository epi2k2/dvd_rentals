-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Altas y bajas de clientes
-- MAGIC
-- MAGIC En este paso se va a cargar los datos necesarios para el departamento de clientes, para poder analizar las altas y las bajas de los clientes.
-- MAGIC
-- MAGIC Se va a recrear la tabla en cada carga.

-- COMMAND ----------

truncate table gold.clientes.subscribed_unsubscribed_clients;

-- COMMAND ----------

insert into gold.clientes.subscribed_unsubscribed_clients
select first_name,
  last_name,
  case
    when active = 1 then true
    else false
  end as active,
  now() as load_date
from silver.customers.link_customer lc
left join silver.customers.sat_customer sc
on lc.customer_key = sc.customer_key and sc.end_date is null;
