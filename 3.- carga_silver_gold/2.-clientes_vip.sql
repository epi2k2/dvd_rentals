-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Clientes VIP
-- MAGIC
-- MAGIC En este paso se va a cargar los datos necesarios para el departamento de clientes, para poder analizar las los clientes más significativos.
-- MAGIC
-- MAGIC Se va a recrear la tabla en cada carga.

-- COMMAND ----------

truncate table gold.clientes.vip_clients;

-- COMMAND ----------

insert into gold.clientes.vip_clients
select lc.first_name,
  lc.last_name,
  sum(amount) as amount_per_month,
  date_format(lr.rental_date, 'yyyy-MM') as date,
  sc.email,
  hco.country,
  hc.city,
  la.address,
  la.address2,
  la.postal_code,
  now() as load_date
from silver.customers.link_customer lc
left join silver.customers.sat_customer sc
on lc.customer_key = sc.customer_key and sc.end_date is null
left join silver.customers.link_address la
on sc.address_key = la.address_key
left join silver.customers.hub_city hc
on la.city_key = hc.city_key
left join silver.customers.link_country__city lcc
on hc.city_key = lcc.city_key
left join silver.customers.hub_country hco
on lcc.country_key = hco.country_key
inner join silver.rentals.link_payment lp
on lc.customer_key = lp.customer_key
inner join silver.rentals.link_rental lr
on lp.rental_key = lr.rental_key
group by lc.first_name,
  lc.last_name,
  sc.email,
  hco.country,
  hc.city,
  la.address,
  la.address2,
  la.postal_code,
  date_format(lr.rental_date, 'yyyy-MM')
order by lc.first_name,
  lc.last_name,
  date_format(lr.rental_date, 'yyyy-MM')
;
