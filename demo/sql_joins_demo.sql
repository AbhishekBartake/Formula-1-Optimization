-- Databricks notebook source
use f1_presentation

-- COMMAND ----------

desc driver_standings

-- COMMAND ----------

create or replace temp view v_driver_standings_2021
as 
select race_year,driver_name,total_points, wins, rank 
from driver_standings
where race_year = 2021


-- COMMAND ----------

select * from v_driver_standings_2021

-- COMMAND ----------

create or replace temp view v_driver_standings_2023
as 
select race_year,driver_name,total_points, wins,rank
from driver_standings
where race_year = 2023


-- COMMAND ----------

select* from v_driver_standings_2023

-- COMMAND ----------

select *
from v_driver_standings_2021 d_2021
join v_driver_standings_2023 d_2023
 on (d_2021.driver_name = d_2023.driver_name)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC left join

-- COMMAND ----------

select *
from v_driver_standings_2021 d_2021
left join v_driver_standings_2023 d_2023
 on (d_2021.driver_name = d_2023.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC right join

-- COMMAND ----------

select *
from v_driver_standings_2021 d_2021
right join v_driver_standings_2023 d_2023
 on (d_2021.driver_name = d_2023.driver_name)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Full join (Everything but matching)

-- COMMAND ----------

select *
from v_driver_standings_2021 d_2021
full join v_driver_standings_2023 d_2023
 on (d_2021.driver_name = d_2023.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Anti Join (left but not right)

-- COMMAND ----------

select *
from v_driver_standings_2021 d_2021
anti join v_driver_standings_2023 d_2023
 on (d_2021.driver_name = d_2023.driver_name)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC cross join (cartesian product)

-- COMMAND ----------

select *
from v_driver_standings_2021 d_2021
cross join v_driver_standings_2023 d_2023


-- COMMAND ----------

