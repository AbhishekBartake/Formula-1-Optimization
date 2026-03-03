-- Databricks notebook source
select * from f1_presentation.calculated_race_results

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Calculating total number of points for each driver and ranking them based on their total number of points 

-- COMMAND ----------

select driver_name,
       count(1) as total_races,
       SUM(calculated_points) as total_points,
       AVG(calculated_points) as avg_points
  from f1_presentation.calculated_race_results
group by driver_name
having count(1)>=50
order by avg_points desc

-- COMMAND ----------

select driver_name,
       count(1) as total_races,
       SUM(calculated_points) as total_points,
       AVG(calculated_points) as avg_points
  from f1_presentation.calculated_race_results
  where race_year between 2011 and 2024
group by driver_name
having count(1)>=50
order by avg_points desc

-- COMMAND ----------

select driver_name,
       count(1) as total_races,
       SUM(calculated_points) as total_points,
       AVG(calculated_points) as avg_points
  from f1_presentation.calculated_race_results
  where race_year between 2001 and 2010
group by driver_name
having count(1)>=50
order by avg_points desc