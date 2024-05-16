-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Silver
-- MAGIC Aquí se va a crear la capa silver. Va a estar formada por 4 esquemas:
-- MAGIC - films
-- MAGIC - rentals
-- MAGIC - customers
-- MAGIC - stores
-- MAGIC
-- MAGIC Se va a seguir la metodología Data Vault, creando tablas HUB, LINK y SATELLITE. Se van a tomar algunas licencias a la hora de crear las claves hash, ya que las tablas vienen en 3ª forma normal. En lugar de usar una clave de negocio (como el título de la película), se van a usar los ids, ya que es la única forma de obtener las relaciones entre tablas.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creación del catálogo

-- COMMAND ----------

create catalog if not exists silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creación de los esquemas

-- COMMAND ----------

create schema if not exists silver.films;
create schema if not exists silver.rentals;
create schema if not exists silver.customers;
create schema if not exists silver.stores;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creación de las tablas del esquema films

-- COMMAND ----------

/*Language*/
create table if not exists silver.films.hub_language(
  language_key string,
  language string,
  rec_src string,
  load_date timestamp,
  constraint pk_hub_language primary key (language_key)
) using delta;

/* Rating*/
create table if not exists silver.films.hub_rating(
  rating_key string,
  rating string,
  rec_src string,
  load_date timestamp,
  constraint pk_hub_rating primary key (rating_key)
) using delta;

/* Film */
create table if not exists silver.films.link_film(
  film_key string,
  film string,
  description string,
  release_year int,
  language_key string,
  length int,  
  rec_src string,
  load_date timestamp,
  constraint pk_link_film primary key (film_key),
  constraint fk_link_film_language_key foreign key (language_key) references silver.films.hub_language
) using delta;

create table if not exists silver.films.sat_film(
  film_key string,
  rental_duration int,
  rental_rate double,
  replacement_cost double,
  rating_key string,
  special_features string,
  full_text string,
  hash_diff string,
  rec_src string,
  start_date timestamp,
  end_date timestamp,
  constraint pk_sat_film primary key (film_key, start_date),
  constraint fk_sat_film_store_key foreign key (film_key) references silver.films.link_film,
  constraint fk_sat_film_rating_key foreign key (rating_key) references silver.films.hub_rating
) using delta;

/*Category*/
create table if not exists silver.films.hub_category(
  category_key string,
  category string,
  rec_src string,
  load_date timestamp,
  constraint pk_hub_category primary key (category_key)
) using delta;

create table if not exists silver.films.link_film__category(
  film__category_key string,
  film_key string,
  category_key string,
  rec_src string,
  load_date timestamp,
  constraint pk_link_film__category primary key (film__category_key),
  constraint fk_link_film__category_film_key foreign key (film_key) references silver.films.link_film,
  constraint fk_link_film__category_category_key foreign key (category_key) references silver.films.hub_category
) using delta;

/*Actor*/
create table if not exists silver.films.hub_actor(
  actor_key string,
  first_name string,
  last_name string,
  rec_src string,
  load_date timestamp,
  constraint pk_hub_actor primary key (actor_key)
) using delta;

create table if not exists silver.films.link_film__actor(
  film__actor_key string,
  film_key string,
  actor_key string,
  hash_diff string,
  rec_src string,
  load_date timestamp,
  constraint pk_link_film__actor primary key (film__actor_key),
  constraint fk_link_film__actor_film_key foreign key (film_key) references silver.films.link_film,
  constraint fk_link_film__actor_actor_key foreign key (actor_key) references silver.films.hub_actor
) using delta;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creación de las tablas del esquema customers

-- COMMAND ----------

/*Country*/
create table if not exists silver.customers.hub_country(
  country_key string,
  country string,
  rec_src string,
  load_date timestamp,
  constraint pk_hub_country primary key (country_key)
) using delta;

/*City*/
create table if not exists silver.customers.hub_city(
  city_key string,
  city string,
  country_id int,
  rec_src string,
  load_date timestamp,
  constraint pk_hub_city primary key (city_key)
) using delta;

create table if not exists silver.customers.link_country__city(
  country__city_key string,
  country_key string,
  city_key string,
  rec_src string,
  load_date timestamp,
  constraint pk_link_country__city primary key (country__city_key),
  constraint fk_link_country__city_country_key foreign key (country_key) references silver.customers.hub_country,
  constraint fk_link_country__city_city_key foreign key (city_key) references silver.customers.hub_city
) using delta;

/*Address*/
create table if not exists silver.customers.link_address(
  address_key string,
  address string,
  address2 string,
  district string,
  city_key string,
  postal_code int,
  phone long,
  rec_src string,
  load_date timestamp,
  constraint pk_link_address primary key (address_key),
  constraint fk_link_address_city_key foreign key (city_key) references silver.customers.hub_city
) using delta;

/*Customer*/
create table if not exists silver.customers.link_customer(
  customer_key string,
  first_name string,
  last_name string,
  create_date date, 
  rec_src string,
  load_date timestamp,
  constraint pk_link_customer primary key (customer_key)
) using delta;

create table if not exists silver.customers.sat_customer(
  customer_key string,
  store_key string,
  email string,
  address_key string,
  active_bool string,
  active int,
  hash_diff string,
  rec_src string,
  start_date timestamp,
  end_date timestamp,
  constraint pk_sat_customer primary key (customer_key, start_date),
  constraint fk_sat_customer_store_key foreign key (customer_key) references silver.customers.link_customer,
  constraint fk_sat_customer_address_key foreign key (address_key) references silver.customers.link_address
) using delta; --Falta FK store_key

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creación de las tablas del esquema stores

-- COMMAND ----------

/*Store*/
create table if not exists silver.stores.hub_store(
  store_key string,
  store_id int,
  rec_src string,
  load_date timestamp,
  constraint pk_hub_store primary key (store_key)
) using delta;

create table if not exists silver.stores.sat_store(
  store_key string,
  manager_staff_key string,
  address_key string,
  hash_diff string,
  rec_src string,
  start_date timestamp,
  end_date timestamp,
  constraint pk_sat_store primary key (store_key, start_date),
  constraint fk_sat_store_store_key foreign key (store_key) references silver.stores.hub_store,
  constraint fk_sat_store_address_key foreign key (address_key) references silver.customers.link_address
) using delta;

alter table silver.customers.sat_customer
drop constraint if exists fk_sat_customer_store_key;

alter table silver.customers.sat_customer
add constraint fk_sat_customer_store_key
foreign key (store_key) 
references silver.stores.hub_store (store_key);

/*Staff*/
create table if not exists silver.stores.hub_staff(
  staff_key string,
  first_name string,
  last_name string,
  rec_src string,
  load_date timestamp,
  constraint pk_hub_staff primary key (staff_key)
) using delta;

create table if not exists silver.stores.sat_staff(
  staff_key string,
  address_key string,
  email string,
  store_key string,
  active string,
  username string,
  password string,
  picture string,
  hash_diff string,
  rec_src string,
  start_date timestamp,
  end_date timestamp,
  constraint pk_sat_staff primary key (staff_key, start_date),
  constraint fk_sat_staff_staff_key foreign key (staff_key) references silver.stores.hub_staff,
  constraint fk_sat_staff_address_key foreign key (address_key) references silver.customers.link_address,
  constraint fk_sat_staff_store_key foreign key (store_key) references silver.stores.hub_store
) using delta;

alter table silver.stores.sat_store
drop constraint if exists fk_sat_store_staff_key;

alter table silver.stores.sat_store
add constraint fk_sat_store_staff_key
foreign key (manager_staff_key) 
references silver.stores.hub_staff (staff_key);

/*Inventory*/
create table if not exists silver.stores.link_inventory(
  inventory_key string,
  inventory_id int,
  film_key string,
  rec_src string,
  load_date timestamp,
  constraint pk_link_inventory primary key (inventory_key),
  constraint fk_link_inventory_film_key foreign key (film_key) references silver.films.link_film
) using delta;

create table if not exists silver.stores.sat_inventory(
  inventory_key string,
  store_key string,
  hash_diff string,
  rec_src string,
  start_date timestamp,
  end_date timestamp,
  constraint pk_sat_inventory primary key (inventory_key, start_date),
  constraint fk_sat_inventory_inventory_key foreign key (inventory_key) references silver.stores.link_inventory,
  constraint fk_sat_inventory_store_key foreign key (store_key) references silver.stores.hub_store
) using delta;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creación de las tablas del esquema rentals

-- COMMAND ----------

/*Rental*/
create table if not exists silver.rentals.link_rental(
  rental_key string,
  rental_date timestamp,
  inventory_key string,
  customer_key string,
  staff_key string,
  rec_src string,
  load_date timestamp,
  constraint pk_link_rental primary key (rental_key),
  constraint fk_link_rental_inventory_key foreign key (inventory_key) references silver.stores.link_inventory,
  constraint fk_link_rental_customer_key foreign key (customer_key) references silver.customers.link_customer,
  constraint fk_link_rental_staff_key foreign key (staff_key) references silver.stores.hub_staff
) using delta;

create table if not exists silver.rentals.sat_rental(
  rental_key string,
  return_date timestamp,
  hash_diff string,
  rec_src string,
  start_date timestamp,
  end_date timestamp,
  constraint pk_sat_rental primary key (rental_key, start_date),
  constraint fk_sat_rental_rental_key foreign key (rental_key) references silver.rentals.link_rental
) using delta;

/*Payment*/
create table if not exists silver.rentals.link_payment(
  payment_key string,
  customer_key string,
  staff_key string,
  rental_key string,
  amount double,
  rec_src string,
  load_date timestamp,
  constraint pk_link_payment primary key (payment_key),
  constraint fk_link_payment_customer_key foreign key (customer_key) references silver.customers.link_customer,
  constraint fk_link_payment_staff_key foreign key (staff_key) references silver.stores.hub_staff,
  constraint fk_link_payment_rental_key foreign key (rental_key) references silver.rentals.link_rental
) using delta;
