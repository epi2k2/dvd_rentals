# DVD Rentals

## Índice
- [DVD Rentals](#dvd-rentals)
  - [Índice](#índice)
  - [Introducción](#introducción)
  - [Estado del proyecto](#estado-del-proyecto)
  - [Estructura del proyecto](#estructura-del-proyecto)
  - [Futuras revisiones](#futuras-revisiones)

## Introducción

Este proyecto es la práctica entregable del curso de Databricks.

## Estado del proyecto

La última actualización es de mayo de 2024.

## Estructura del proyecto

Está dividido en 5 carpetas:
- 0.- creacion_bbdd: aquí se encuentra la creación de toda la BBDD
- 1.- carga_landing_bronze: aquí está el código para copiar los ficheros desde landing hasta la capa bronze
- 2.- carga_bronze_silver: en esta carpeta está el código para nutrir las tablas de la capa silver
- 3.- carga_silver_gold: contiene el código para generar el contenido de las tablas gold
- 4.- carga_gold_departing: aquí está el código para generar los ficheros de departing

## Futuras revisiones

Para futuras revisiones se debería hacer:
- Cambiar los notebooks por scripts de python
- Generar módulos de python para reutilizar código
