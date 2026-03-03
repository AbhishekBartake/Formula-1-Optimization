-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC create circuits table

-- COMMAND ----------

drop table if exists f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
using csv
options(path "abfss://raw@f1dl3.dfs.core.windows.net/circuits.csv",header true)

-- COMMAND ----------

select * from f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC now the races table

-- COMMAND ----------

drop table if exists f1_raw.races;
create table if not exists f1_raw.races(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING,
  fp1_date DATE,
  fp1_time STRING,
  fp2_date DATE,
  fp2_time STRING,
  fp3_date DATE,
  fp3_time STRING,
  quali_date DATE,
  quali_time STRING,
  sprint_date DATE,
  sprint_time STRING
)
using csv
options(path "abfss://raw@f1dl3.dfs.core.windows.net/races.csv",header true)

-- COMMAND ----------

select * from f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC create tables for JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC constructors file

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
create table if not exists f1_raw.constructors(
  constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING
)
using json
options(path "abfss://raw@f1dl3.dfs.core.windows.net/constructors.json")

-- COMMAND ----------

select * from f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC drivers file

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
create table if not exists f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename:STRING,surname:STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
using json 
options(path "abfss://raw@f1dl3.dfs.core.windows.net/drivers.json")

-- COMMAND ----------

select * from f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC results table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
create table if not exists f1_raw.results(
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points FLOAT,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed STRING,
  statusId INT
)
using json
options(path "abfss://raw@f1dl3.dfs.core.windows.net/results.json")


-- COMMAND ----------

select * from f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC pit stops file

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
create table if not exists f1_raw.pit_stops(
  raceId INT,
  driverId INT,
  stop INT,
  lap INT,
  time STRING,
  duration STRING,
  milliseconds INT
)
using JSON
options(path "abfss://raw@f1dl3.dfs.core.windows.net/pit_stops.json",multiLine true)

-- COMMAND ----------

select * from f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC reading from multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
create table if not exists f1_raw.lap_times(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
using CSV
options(path "abfss://raw@f1dl3.dfs.core.windows.net/lap_times",header true)

-- COMMAND ----------

select count(*) from f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC qualifying file's'

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
create table if not exists f1_raw.qualifying(
  qualifyId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING
)
using json
options(path "abfss://raw@f1dl3.dfs.core.windows.net/qualifying",multiLine true)

-- COMMAND ----------

select * from f1_raw.qualifying

-- COMMAND ----------

desc extended f1_raw.qualifying