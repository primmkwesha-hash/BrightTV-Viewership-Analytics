-- Databricks notebook source
select * 
From `workspace`.`brighttv-analytics`.`user_profiles`;


-- COMMAND ----------

--Check total rows vs unique users, to find duplicates
SELECT 
  COUNT(*) AS total_rows_in_profile,
  COUNT(DISTINCT UserID) AS unique_users,
  COUNT(*) - COUNT(DISTINCT UserID) AS duplicate_userID_count
  From `workspace`.`brighttv-analytics`.`user_profiles`;


-- COMMAND ----------

--Missing Values Check
SELECT
  SUM(CASE WHEN UserID IS NULL THEN 1 ELSE 0 END) AS null_UserID,
  SUM(CASE WHEN Age IS NULL THEN 1 ELSE 0 END) AS null_Age,
  SUM(CASE WHEN Gender IS NULL THEN 1 ELSE 0 END) AS null_Gender,
  SUM(CASE WHEN Race IS NULL THEN 1 ELSE 0 END) AS null_Race,
  SUM(CASE WHEN Province IS NULL THEN 1 ELSE 0 END) AS null_Province,
  SUM(CASE WHEN Email IS NULL THEN 1 ELSE 0 END) AS null_Email
FROM `workspace`.`brighttv-analytics`.`user_profiles`;

-- COMMAND ----------

--Age Distribution for Age_Bucket validation
SELECT
  MIN(Age) AS min_age,
  MAX(Age) AS max_age,
  AVG(Age) AS avg_age,
  -- Count how many users fall into each bucket
  SUM(CASE WHEN Age BETWEEN 11 AND 19 THEN 1 ELSE 0 END) AS Teenager_11_19,
  SUM(CASE WHEN Age BETWEEN 20 AND 39 THEN 1 ELSE 0 END) AS YoungAdult_20_39,
  SUM(CASE WHEN Age BETWEEN 40 AND 59 THEN 1 ELSE 0 END) AS Adult_40_59,
  SUM(CASE WHEN Age >= 60 THEN 1 ELSE 0 END) AS Senior_60plus,
  SUM(CASE WHEN Age < 11 OR Age > 100 THEN 1 ELSE 0 END) AS invalid_age_outlier
FROM `workspace`.`brighttv-analytics`.`user_profiles`;

-- COMMAND ----------

--Checking Province distribution
Select Province, count(*) As Users
From `workspace`.`brighttv-analytics`.`user_profiles`
Group by Province
Order by Users Desc;


-- COMMAND ----------

--Check empty spaces
Select *
From `workspace`.`brighttv-analytics`.`user_profiles`
where Name = ' ' or Surname = ' ' or Email = ' ' or Gender = ' ' or Race = ' ' or Province = ' ' or `Social Media Handle` = ' ';

-- COMMAND ----------

--Check for none
Select *
From `workspace`.`brighttv-analytics`.`user_profiles`
where Name = 'None' or Surname = 'None' or Email = 'None' or Gender = 'None' or Race = 'None' or Province = 'None' or `Social Media Handle` = 'None';

-- COMMAND ----------

--Evaluate Demographic Profile
Select Gender, Race, Count(*)
From `workspace`.`brighttv-analytics`.`user_profiles`
Group by Gender, Race
Order by Count(*) Desc;


-- COMMAND ----------

Select * 
From `workspace`.`brighttv-analytics`.`viewership`;

-- COMMAND ----------

--Checking the date range of the data
Select Min(RecordDate2) As Min_Date, 
      Max(RecordDate2) As Max_Date
From `workspace`.`brighttv-analytics`.`viewership`;

-- COMMAND ----------

--Convert UTC to SA timezone
Select DATEADD(Hour,2,RecordDate2) As SA_Time
From `workspace`.`brighttv-analytics`.`viewership`;

-- COMMAND ----------

--Remove duplicates
Select UserID0 As UserID
From `workspace`.`brighttv-analytics`.`viewership`;

-- COMMAND ----------

--How many viewing sessions happened at each exact date and time
Select RecordDate2, Count(*)
From `workspace`.`brighttv-analytics`.`viewership`
Group By RecordDate2
Order By RecordDate2 Desc;

-- COMMAND ----------

--Total sessions
Select Count(*) As Total_Sessions
From `workspace`.`brighttv-analytics`.`viewership`;


-- COMMAND ----------

--How to count actual users
Select 
Count(Distinct UserID0) As Total_Users
From `workspace`.`brighttv-analytics`.`viewership`;


-- COMMAND ----------

--Sessions per users
Select UserID0, Count(*) As Sessions
From `workspace`.`brighttv-analytics`.`viewership`
Group By UserID0
Order By Sessions Desc;

-- COMMAND ----------

--Most watched channels
Select Channel2, Count(*) As Views
From `workspace`.`brighttv-analytics`.`viewership`
Group By Channel2
Order By Views Desc;

-- COMMAND ----------

--Average duration per channel
Select Channel2, Avg(`Duration 2`) As AvgDuration
From `workspace`.`brighttv-analytics`.`viewership`
Group By Channel2;

-- COMMAND ----------

--Check the number of users per day
Select RecordDate2, Count(RecordDate2) As daily_users
From `workspace`.`brighttv-analytics`.`viewership`
Group By RecordDate2
Order By daily_users Desc;

-- COMMAND ----------

--Joining the tables using the UserID column
-- Returns all records from viewership table (left table) + matching user profiles data

SELECT
  v.*, 
  u.UserID, 
  u.Age, 
  u.Gender, 
  u.Race, 
  u.Province 
FROM `workspace`.`brighttv-analytics`.`viewership` AS v 
LEFT JOIN `workspace`.`brighttv-analytics`.`user_profiles` AS u 
  ON v.UserID0 = u.UserID; 

-- COMMAND ----------


 SELECT
  U.UserID,
  v.RecordDate2,
  v.Channel2,
  v.`Duration 2`,
  u.Age,
  u.Race,
  u.Gender,
  u.Province,
  -- Time transformation
  DATEADD(Hour, 2, v.RecordDate2) AS SA_Time,
  -- Time bucketing(Group watch times into parts of the day)
  CASE
  WHEN date_format(v.RecordDate2, 'HH:mm:ss') BETWEEN '00:00:00' AND '05:59:59' THEN 'Early Morning'
  WHEN date_format(v.RecordDate2, 'HH:mm:ss') BETWEEN '06:00:00' AND '11:59:59' THEN 'Morning'
  WHEN date_format(v.RecordDate2, 'HH:mm:ss') BETWEEN '12:00:00' AND '17:59:59' THEN 'Afternoon'
  WHEN date_format(v.RecordDate2, 'HH:mm:ss') BETWEEN '18:00:00' AND '21:59:59' THEN 'Evening'
  Else 'Night'
  End AS Time_Bucket,
  --Age bucketing(Group users into life stages)
  CASE
  WHEN u.Age BETWEEN 11 AND 19 THEN 'Teenager'
  WHEN u.Age BETWEEN 20 AND 39 THEN 'Young Adult'
  WHEN u.Age BETWEEN 40 AND 59 THEN 'Adult'
  ELSE'Senior'
  END AS Age_Bucket,
  --Aggregate Metrics
  Count(v.Channel2) As Views,
  COUNT(*) AS total_sessions,
  SUM(v.`Duration 2`) AS total_minutes,
  AVG(v.`Duration 2`) AS avg_session_duration,
  MAX(v.`Duration 2`) AS max_minutes,
  MIN(v.`Duration 2`) AS min_minutes
  --Table joins
  FROM `workspace`.`brighttv-analytics`.`viewership` As v
  LEFT JOIN `workspace`.`brighttv-analytics`.`user_profiles` As u
  On V.UserID0 = U.UserID
  GROUP BY 
  Age_Bucket, 
  Time_Bucket,
  u.Gender,
  v.Channel2,
  u.Province,
  v.RecordDate2,
  u.UserID,
  u.Age,
  u.Race,
  v.`Duration 2`
  Order By  
  Age_Bucket,
  Time_Bucket,
  v.Channel2,
  u.Gender;