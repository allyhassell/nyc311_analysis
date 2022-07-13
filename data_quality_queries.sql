# 1. Zips in ref data
CREATE TABLE aha199_623.`1.Zip_code_in_ref` (SELECT zip_code_nyc_borough.`Zip`, sr_incident_zip_summary.`Count`
FROM sr_incident_zip_summary, zip_code_nyc_borough
WHERE sr_incident_zip_summary.`Incident Zip` = zip_code_nyc_borough.`Zip`
ORDER BY sr_incident_zip_summary.`Incident Zip`);

CREATE TABLE aha199_623.`1.Result` (SELECT sum(`Count`) AS `Total`, 
(SELECT sum(`Count`) FROM aha199_623.`1.Zip_code_in_ref`) AS `Valid entires`,
(SELECT sum(`Count`) FROM aha199_623.`1.Zip_code_in_ref`) * 100 / (sum(`Count`)) AS `Percentage`
from sr_incident_zip_summary);

# 2. Zip requests are NULL or empty
SELECT sum(`Count`) AS `Total`, 
(SELECT sum(`Count`) FROM sr_incident_zip_summary WHERE `Incident Zip` IS NOT NULL) as `Valid entires`,
(SELECT sum(`Count`) FROM sr_incident_zip_summary WHERE `Incident Zip` IS NOT NULL) * 100 / (sum(`Count`)) AS `Percentage`
FROM sr_incident_zip_summary;

CREATE TABLE aha199_623.`2.Result` (SELECT sum(`Count`) AS `Total`, 
(sum(`Count`) - (SELECT sum(`Count`) FROM sr_incident_zip_summary WHERE `Incident Zip` = '')) AS `Valid entires`,
(sum(`Count`) - (SELECT sum(`Count`) FROM sr_incident_zip_summary WHERE `Incident Zip` = '')) * 100 / (sum(`Count`)) AS `Percentage`
FROM sr_incident_zip_summary);

# 3. Not in NY but still valid zip codes
CREATE TABLE aha199_623.`3.Valid_zip_codes_not_in_NY` 
(WITH not_ny_zips AS (SELECT * 
FROM sr_incident_zip_summary
WHERE (`Incident Zip` < 1001 OR `Incident Zip` > 11697) AND (`Incident Zip` < 99950) AND (`Incident Zip` > 0))

SELECT *
FROM not_ny_zips
WHERE `Incident Zip` REGEXP '^[0-9]{5}$');

CREATE TABLE aha199_623.`3.Result` (SELECT sum(`Count`) AS `Total`, 
(SELECT sum(`Count`) FROM aha199_623.`3.Valid_zip_codes_not_in_NY`) AS `Zip codes not in NY`,
(100 - (SELECT sum(`Count`) FROM aha199_623.`3.Valid_zip_codes_not_in_NY`) * 100 / (sum(`Count`))) AS `Percentage valid on this dimension`,
(SELECT sum(`Count`) FROM aha199_623.`3.Valid_zip_codes_not_in_NY`) * 100 / (sum(`Count`)) AS `Percentage not NY but valid`
FROM sr_incident_zip_summary);

# 4. Filter out incorrect entries with symbols using regular expressions
CREATE TABLE aha199_623.`4.Sr_not_symbols` (SELECT *
FROM sr_complaint_type_summary
WHERE `Complaint Type` NOT REGEXP '[!@#$%^&*_+=:"?;.{}~`]|Misc. Comments'
ORDER BY `Count` DESC);

CREATE TABLE aha199_623.`4.Result` (SELECT sum(`Count`) AS `Total`, 
(SELECT sum(`Count`) FROM aha199_623.`4.Sr_not_symbols`) AS `Valid entires`,
(SELECT sum(`Count`) FROM aha199_623.`4.Sr_not_symbols`) * 100 / (sum(`Count`)) AS `Percentage`
from sr_complaint_type_summary);

# 5. SR compared with key words from both open data and prob areas 
CREATE TABLE aha199_623.`5.Sr_contains_keywords_problem_areas_open_26` (SELECT *
FROM aha199_623.`4.Sr_not_symbols`
WHERE `Complaint Type` REGEXP 'Blocked|Driveway|Broken|Muni|Building|Damaged|Tree|Dirty|Conditions|Electric|General|Construction|Construction|Plumbing|Heating|Illegal|Parking|Noise|Residential|Street|Sidewalk|Nonconst|Overgrown|Tree|Branches|Paint|Plaster|Plumbing|Rodent|Sanitation|Condition|Sewer|Sidewalk|Condition|Street|Condition|Street|Light|Condition|Traffic|Signal|Condition|Water|System|Animals|Apartments|Buildings|Beach|Pool|Sauna|Benefit|Card|Replacement|Bridges|Stop|Shelter|Cable|Collections|Cooling|Tower|Derelict|Bicycle|DRIE|Ferry|Food|Establishment|Vehicle|Hazardous|Waste|Material|Health|Highways|Homebound|Evacuation|Homeless|Kit|Link|Kiosks|Literature|Litter|Spill|Parking|Facility|Parking|Meters|Parking|Tickets|Parks|Maintenance|Payphone|Personnel|Property|Taxes|Quality|Life|Rodent|Sanitation|Condition|Sanitation|Enforcement|School|Maintenance|SCRIE|Seniors|Sewer|Sidewalks|Snow|Standing|Water|Street|Signs|Streets|Tax|Payer|Advocate|Taxi|Lost|Found|Traffic|Parking|Trees|Tunnels|Vehicles|Water'
OR `Complaint Type`  REGEXP ' (Air|Meter|Day|Care|Bus|Hire|Lead|Oil|Gas) ' OR `Complaint Type`  REGEXP ' (Air|Meter|Day|Care|Bus|Hire|Lead|Oil|Gas)$' OR `Complaint Type`  REGEXP '^(Air|Meter|Day|Care|Bus|Hire|Lead|Oil|Gas) ' 
ORDER BY `Count` DESC);

CREATE TABLE aha199_623.`5.Result` (SELECT sum(`Count`) AS `Total`, 
(SELECT sum(`Count`) FROM aha199_623.`5.Sr_contains_keywords_problem_areas_open_26`) AS `Valid entires`,
(SELECT sum(`Count`) FROM aha199_623.`5.Sr_contains_keywords_problem_areas_open_26`) * 100 / (sum(`Count`)) AS `Percentage`
FROM sr_complaint_type_summary);

# 6. Service request directly matching either problem areas or open 26 
CREATE TABLE aha199_623.`6.Ref_both_problem_areas_and_open_data` 
	SELECT `problem_area` FROM ref_problem_areas_nyc311_portal_57
		UNION 
	SELECT `Type` FROM ref_sr_type_nyc311_open_data_26;
    
CREATE TABLE aha199_623.`6.Sr_in_problem_areas_and_open_data` (SELECT sr_complaint_type_summary.`Complaint Type` , aha199_623.`6.Ref_both_problem_areas_and_open_data`.`problem_area`, sr_complaint_type_summary.`Count`
FROM sr_complaint_type_summary
LEFT JOIN aha199_623.`6.Ref_both_problem_areas_and_open_data` ON aha199_623.`6.Ref_both_problem_areas_and_open_data`.`problem_area` LIKE sr_complaint_type_summary.`Complaint Type`
WHERE aha199_623.`6.Ref_both_problem_areas_and_open_data`.`problem_area` IS NOT NULL
ORDER BY `Count` DESC);

CREATE TABLE aha199_623.`6.Result` (SELECT sum(`Count`) AS `Total`, 
(SELECT sum(`Count`) FROM aha199_623.`6.Sr_in_problem_areas_and_open_data`) AS `Valid entires`,
(SELECT sum(`Count`) FROM aha199_623.`6.Sr_in_problem_areas_and_open_data`) * 100 / (sum(`Count`)) AS `Percentage`
FROM sr_complaint_type_summary);

# 7. Service request values which are NULL 
CREATE TABLE aha199_623.`7.Result` (SELECT sum(`Count`) AS `Total`, 
(SELECT sum(`Count`) FROM sr_complaint_type_summary WHERE `Complaint Type` IS NOT NULL) AS `Valid entires`,
(SELECT sum(`Count`) FROM sr_complaint_type_summary where `Complaint Type` IS NOT NULL) * 100 / (sum(`Count`)) AS `Percentage`
FROM sr_complaint_type_summary);

# 8. Checks uniquness of service request unique key
CREATE TABLE aha199_623.`8.Result` (SELECT count(DISTINCT `Unique Key`), count(`Unique Key`)
FROM `service_request_sample_10k`);

# 9. Checks uniquness of service request assuming Created date/complaint type/address same is incorrect 
CREATE TABLE aha199_623.`9.Sr_duplicates` (SELECT `Created Date`, 	`Complaint Type`, `Incident Address`, count(*) as `Count`
FROM service_request_sample_10k
GROUP BY `Created Date`, 	`Complaint Type`, `Incident Address`
HAVING count(*) > 1);

CREATE TABLE aha199_623.`9.Result` (SELECT count(*) AS `Total`, 
((count(*)) - (SELECT sum(`Count`) FROM aha199_623.`9.Sr_duplicates`)) AS `Valid entires`,
((count(*)) - (SELECT sum(`Count`) FROM aha199_623.`9.Sr_duplicates`)) * 100 / (count(*)) AS `Percentage`
FROM service_request_sample_10k);

# 10. Check closed or open dates are before 01/01/2010 or in future
CREATE TABLE aha199_623.`10.Created_closed_dates_not_before_2010_or_in_future` (SELECT *
FROM service_request_sample_10k
WHERE NOT `Created Date` > '2022-01-01 00:00:00' OR NOT `Created Date` < '2010-01-01 00:00:00' OR NOT `Closed Date` > '2022-01-01 00:00:00' OR NOT `Closed Date` < '2010-01-01 00:00:00' OR NOT `Due date` < '2010-01-01 00:00:00');

CREATE TABLE aha199_623.`10.Result` (SELECT count(*) AS `Total`, 
(SELECT count(*) FROM aha199_623.`10.Created_closed_dates_not_before_2010_or_in_future`) AS `Valid entires`,
(SELECT count(*) FROM aha199_623.`10.Created_closed_dates_not_before_2010_or_in_future`) * 100 / (count(*)) AS `Percentage`
FROM `service_request_sample_10k`);

# 11. Domain check of dates
DESCRIBE service_request_sample_10k '%Date%';

# 12. Any date is null
CREATE TABLE aha199_623.`12.Created_date_not_null` (SELECT `Unique Key`, `Created Date`
FROM service_request_sample_10k
WHERE service_request_sample_10k.`Created Date` IS NOT NULL);

CREATE TABLE aha199_623.`12.Closed_date_not_null` (SELECT `Unique Key`, `Closed Date`
FROM service_request_sample_10k
WHERE service_request_sample_10k.`Closed Date` IS NOT NULL);

CREATE TABLE aha199_623.`12.Due_date_not_null` (SELECT `Unique Key`,`Due Date`
FROM service_request_sample_10k
WHERE service_request_sample_10k.`Due Date` IS NOT NULL);

CREATE TABLE aha199_623.`12.All_dates_not_null` (SELECT `Unique Key`, `Created Date`, `Closed Date`, `Due Date`
FROM service_request_sample_10k
WHERE service_request_sample_10k.`Created Date` IS NOT NULL and service_request_sample_10k.`Closed Date` IS NOT NULL and service_request_sample_10k.`Due Date` IS NOT NULL);

CREATE TABLE aha199_623.`12.Result` (SELECT count(*) AS `Total`, 
(SELECT count(*) FROM aha199_623.`12.Created_date_not_null`) AS `Valid Created Date entires`,
(SELECT count(*) FROM aha199_623.`12.Closed_date_not_null`) AS `Valid Closed Date entires`,
(SELECT count(*) FROM aha199_623.`12.Due_date_not_null`) AS `Valid Due Date entires`,
(SELECT count(*) FROM aha199_623.`12.All_dates_not_null`) AS `Valid entires all dates`,
(SELECT count(*) FROM aha199_623.`12.Created_date_not_null`)  * 100 / (count(*)) AS `Percentage of Created Date entires`,
(SELECT count(*) FROM aha199_623.`12.Closed_date_not_null`)  * 100 / (count(*)) AS `Percentage of Closed Date entires`,
(SELECT count(*) FROM aha199_623.`12.Due_date_not_null`)  * 100 / (count(*)) AS `Percentage of Due Date entires`, 
(SELECT count(*) FROM aha199_623.`12.All_dates_not_null`)  * 100 / (count(*)) AS `Percentage of all entires`
FROM `service_request_sample_10k`);

# 13. Veiw all results tables created to validate code 
SELECT * FROM aha199_623.`1.Result`;
SELECT * FROM aha199_623.`2.Result`;
SELECT * FROM aha199_623.`3.Result`;
SELECT * FROM aha199_623.`4.Result`;
SELECT * FROM aha199_623.`5.Result`;
SELECT * FROM aha199_623.`6.Result`;
SELECT * FROM aha199_623.`7.Result`;
SELECT * FROM aha199_623.`8.Result`;
SELECT * FROM aha199_623.`9.Result`;
SELECT * FROM aha199_623.`10.Result`;
SELECT * FROM aha199_623.`12.Result`;

SELECT * FROM aha199_623.`1.Zip_code_in_ref`;
SELECT * FROM aha199_623.`3.Valid_zip_codes_not_in_NY`;
SELECT * FROM aha199_623.`4.Sr_not_symbols`;
SELECT * FROM aha199_623.`5.Sr_contains_keywords_problem_areas_open_26`;
SELECT * FROM aha199_623.`6.Ref_both_problem_areas_and_open_data`;
SELECT * FROM aha199_623.`6.Sr_in_problem_areas_and_open_data`;
SELECT * FROM aha199_623.`9.Sr_duplicates`;
SELECT * FROM aha199_623.`10.Created_closed_dates_not_before_2010_or_in_future`;
SELECT * FROM aha199_623.`12.All_dates_not_null`;
SELECT * FROM aha199_623.`12.Closed_date_not_null`;
SELECT * FROM aha199_623.`12.Created_date_not_null`;
SELECT * FROM aha199_623.`12.Due_date_not_null`;
