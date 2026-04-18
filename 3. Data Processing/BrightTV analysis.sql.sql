-- Databricks notebook source


  SELECT
  U.UserID,
  v.RecordDate2,
  v.Channel2,
  v.`Duration 2`,
  u.Age,
  u.Race,
  u.Gender,
  u.Province,
  DATEADD(Hour, 2, v.RecordDate2) AS SA_Time,
  CASE
  WHEN date_format(v.RecordDate2, 'HH:mm:ss') BETWEEN '00:00:00' AND '05:59:59' THEN 'Early Morning'
  WHEN date_format(v.RecordDate2, 'HH:mm:ss') BETWEEN '06:00:00' AND '11:59:59' THEN 'Morning'
  WHEN date_format(v.RecordDate2, 'HH:mm:ss') BETWEEN '12:00:00' AND '17:59:59' THEN 'Afternoon'
  WHEN date_format(v.RecordDate2, 'HH:mm:ss') BETWEEN '18:00:00' AND '21:59:59' THEN 'Evening'
  Else 'Night'
  End AS Time_Bucket,
  CASE
  WHEN u.Age BETWEEN 11 AND 19 THEN 'Teenager'
  WHEN u.Age BETWEEN 20 AND 39 THEN 'Young Adult'
  WHEN u.Age BETWEEN 40 AND 59 THEN 'Adult'
  ELSE'Senior'
  END AS Age_Bucket,
  Count(v.Channel2) As Views,
  COUNT(*) AS total_sessions,
  SUM(v.`Duration 2`) AS total_minutes,
  AVG(v.`Duration 2`) AS avg_session_duration,
  MAX(v.`Duration 2`) AS max_minutes,
  MIN(v.`Duration 2`) AS min_minutes
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