-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Películas más alquiladas
-- MAGIC
-- MAGIC En este paso se va a cargar los datos necesarios para el departamento de mercado para poder analizar las películas más alquiladas por fecha.
-- MAGIC
-- MAGIC Se va a recrear la tabla en cada carga.

-- COMMAND ----------

truncate table gold.mercado.most_rented_movies;

-- COMMAND ----------

insert into gold.mercado.most_rented_movies
select hc.category,
  lf.film,
  lr.rental_date,
  hst2.first_name as staff_first_name,
  hst2.last_name as staff_last_name,
  hs.store_id,
  hst.first_name as manager_first_name,
  hst.last_name manager_last_name,
  now() as load_date
from silver.rentals.link_rental lr
left join silver.stores.link_inventory li
on lr.inventory_key = li.inventory_key
left join silver.films.link_film lf
on li.film_key = lf.film_key
left join silver.stores.sat_inventory si
on li.inventory_key = si.inventory_key and si.end_date is null
left join silver.stores.hub_store hs
on si.store_key = hs.store_key
left join silver.stores.sat_store ss
on hs.store_key = ss.store_key and ss.end_date is null
left join silver.stores.hub_staff hst
on ss.manager_staff_key = hst.staff_key
left join silver.stores.hub_staff hst2
on lr.staff_key = hst2.staff_key
left join silver.films.link_film__category lfc
on lf.film_key = lfc.film_key
left join silver.films.hub_category hc
;
