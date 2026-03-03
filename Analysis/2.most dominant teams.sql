-- Databricks notebook source
select * from f1_presentation.calculated_race_results

-- COMMAND ----------

select team_name,
       sum(calculated_points) as total_points,
       count(1) as total_races,
       round(avg(calculated_points),2) as avg_points
from f1_presentation.calculated_race_results
where race_year between 2001 and 2011
group by team_name
having count(1)>=100
order by avg_points desc