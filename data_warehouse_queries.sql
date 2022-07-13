/* Ally Hassell – MBIS623 Data Warehouse Design */


/* --------------1. Extend dim_yearweek with added records------------------------------------ */
insert into DIM_YEARWEEK
select distinct (yearofweek("Created_Date") * 100 + weekiso("Created_Date")) as yearweek
from nyc311."SERVICE_REQUEST_UPDATES"
order by yearweek;

/* --------------2. Extend fact_service_quality with added records----------------------------  */
/* Create sr_full table from nyc311 service request data */
create or replace view sr_full as
select "Unique_Key" as unique_key,
yearweek,
"Created_Date" as created_date, "Closed_Date" as closed_date, "Agency" as agency,
"Agency_Name" as agency_name,
"Type_ID" as complaint_type_id, "Descriptor" as descriptor, "Location_Type" as location_type,
"Zip" as incident_zip_id, "Incident_Address" as incident_address, "Street_Name" as street_name,
"Cross_Street_1" as cross_street_1, "Cross_Street_2" as cross_street_2,
"Intersection_Street_1" as intersection_street_1, "Intersection_Street_2" as intersection_street_2,
"Address_Type" as address_type, "City" as city, "Landmark" as landmark,
"Facility_Type" as facility_type, "Status" as status, "Due_Date" as due_date,
"Resolution_Description" as resulution_description,
"Resolution_Action_Updated_Date" as resolution_action_updated_date,
"Community_Board" as community_board, "BBL" as bbl, "Borough" as borough,
"X_Coordinate_(State Plane)" as x_coordinate_state_plane,
"Y_Coordinate_(State Plane)" as y_coordinate_state_plane,
"Open_Data_Channel_Type" as open_data_channel_type,
"Park_Facility_Name" as park_facility_name, "Park_Borough" as park_borough, "Vehicle_Type" as vehicle_type,
"Taxi_Company_Borough" as taxi_company_borough,
"Taxi_Pick_Up_Location" as taxi_pick_up_location, "Bridge_Highway_Name" as bridge_highway_name,
"Bridge_Highway_Direction" as bridge_highway_direction,
"Road_Ramp" as road_ramp, "Bridge_Highway_Segment" as bridge_highway_segment,
"Latitude" as latitude, "Longitude" as longitude, "Location" as location
from nyc311."SERVICE_REQUEST"
left join map_complaint_type_open_nyc311
on map_complaint_type_open_nyc311.complaint_type = lower(nyc311."SERVICE_REQUEST"."Complaint_Type")
left join map_incident_zip_nyc_borough
on map_incident_zip_nyc_borough."Zip" = nyc311."SERVICE_REQUEST"."Incident_Zip";
select count(*) from sr_full; 
select count(*) from nyc311.service_request; 

/* Create fact_service_quality */
create or replace table fact_service_quality (
agency_id number(8, 0) not null,
location_zip varchar(5) NOT null,
type_id number(8, 0) not null,
yearweek number(8, 0) not null,
count int not null default 0,
total int default null,
avg float default null,
min int default null,
max int default null,
primary key (agency_id, location_zip, type_id,yearweek)
, constraint agency_dim foreign key (agency_id) references dim_agency (agency_id)
, constraint location_dim foreign key (location_zip) references dim_location (location_zip)
, constraint quest_type_dim foreign key (type_id) references dim_request_type (type_id)
, constraint yearweek_dim foreign key (yearweek) references dim_yearweek (yearweek)
);

/* Insert details into fact_service_quality */
insert into fact_service_quality (agency_id, location_zip, type_id, yearweek, count, total, avg, min, max)
select dim_agency.agency_id, dim_location.location_zip, dim_request_type.type_id,
sr_full.yearweek,
count(*),
sum(timestampdiff(hour, created_date, closed_date)),
avg(timestampdiff(hour, created_date, closed_date)),
min(timestampdiff(hour, created_date, closed_date)),
max(timestampdiff(hour, created_date, closed_date))
from sr_full
inner join dim_agency dim_agency on sr_full.Agency = dim_agency.agency_name
inner join dim_location dim_location on sr_full.incident_zip_id = dim_location.location_zip
inner join dim_request_type dim_request_type on sr_full.complaint_type_id = dim_request_type.type_id
inner join dim_yearweek dim_yearweek on sr_full.yearweek = dim_yearweek.yearweek
group by dim_agency.agency_id, dim_location.location_zip, dim_request_type.type_id, sr_full.yearweek;

/* Create table with service requests updates using infor from nyc311*/
create or replace view sr_updates as
select "Unique_Key" as unique_key,
yearweek,
"Created_Date" as created_date, "Closed_Date" as closed_date, "Agency" as agency,
"Agency_Name" as agency_name,
"Type_ID" as complaint_type_id, "Descriptor" as descriptor, "Location_Type" as location_type,
"Zip" as incident_zip_id, "Incident_Address" as incident_address, "Street_Name" as street_name,
"Cross_Street_1" as cross_street_1, "Cross_Street_2" as cross_street_2,
"Intersection_Street_1" as intersection_street_1, "Intersection_Street_2" as intersection_street_2,
"Address_Type" as address_type, "City" as city, "Landmark" as landmark,
"Facility_Type" as facility_type, "Status" as status, "Due_Date" as due_date,
"Resolution_Description" as resulution_description,
"Resolution_Action_Updated_Date" as resolution_action_updated_date,
"Community_Board" as community_board, "BBL" as bbl, "Borough" as borough,
"X_Coordinate_(State Plane)" as x_coordinate_state_plane,
"Y_Coordinate_(State Plane)" as y_coordinate_state_plane,
"Open_Data_Channel_Type" as open_data_channel_type,
"Park_Facility_Name" as park_facility_name, "Park_Borough" as park_borough, "Vehicle_Type" as vehicle_type,
"Taxi_Company_Borough" as taxi_company_borough,
"Taxi_Pick_Up_Location" as taxi_pick_up_location, "Bridge_Highway_Name" as bridge_highway_name,
"Bridge_Highway_Direction" as bridge_highway_direction,
"Road_Ramp" as road_ramp, "Bridge_Highway_Segment" as bridge_highway_segment,
"Latitude" as latitude, "Longitude" as longitude, "Location" as location
from nyc311."SERVICE_REQUEST_UPDATES"
left join map_complaint_type_open_nyc311
on map_complaint_type_open_nyc311.complaint_type = lower(nyc311."SERVICE_REQUEST_UPDATES"."Complaint_Type")
left join map_incident_zip_nyc_borough
on map_incident_zip_nyc_borough."Zip" = nyc311."SERVICE_REQUEST_UPDATES"."Incident_Zip";

/* Insert these service request updates into the fact sheet */    
insert into fact_service_quality (agency_id, location_zip, type_id, yearweek, count, total, avg, min, max)
select dim_agency.agency_id, dim_location.location_zip, dim_request_type.type_id,
sr_updates.yearweek,
count(*),
sum(timestampdiff(hour, created_date, closed_date)),
avg(timestampdiff(hour, created_date, closed_date)),
min(timestampdiff(hour, created_date, closed_date)),
max(timestampdiff(hour, created_date, closed_date))
from sr_updates
inner join dim_agency dim_agency on sr_updates.Agency = dim_agency.agency_name
inner join dim_location dim_location on sr_updates.incident_zip_id = dim_location.location_zip
inner join dim_request_type dim_request_type on sr_updates.complaint_type_id = dim_request_type.type_id
inner join dim_yearweek dim_yearweek on sr_updates.yearweek = dim_yearweek.yearweek
group by dim_agency.agency_id, dim_location.location_zip, dim_request_type.type_id, sr_updates.yearweek;

/* --------------3a. Average service request processing time for each agency-----------------  */
/* Determine the average time per agency and join with agency id to get the agency names  */
select agency_name, round(sum(total)/sum(count), 2) Average
from fact_service_quality as f
join dim_agency as a
on f.agency_id = a.agency_id
group by agency_name;

/* --------------3b. Average service request processing time in each NYC borough-------------- */
/* Determine the average time per Borough from fact sheet and join to get Borough name */
select "Borough" Borough, round(sum(total)/sum(count), 2) "Average Time"
from fact_service_quality as f
join nyc311.zip_code_nyc_borough as z
on f.location_zip = z."Zip"
group by Borough;

/* --------------3c. Total number of requests for each month of the year----------------------  */
/* Create table identical to sr_full  */
create table aha199_mbis623.sr_complete as select * from sr_full;

/* Insert in sr_updates to form a complete table of service requests */
insert into aha199_mbis623.sr_complete (unique_key, yearweek, created_date, closed_date, agency, agency_name)
select unique_key, yearweek, created_date, closed_date, agency, agency_name from sr_updates;

/* Create table which turns year weeks into year months */
create table aha199_mbis623.monthyear_map as select distinct yearweek, to_varchar(created_date, 'yyyy-MM') as YEARMONTH
from sr_complete
order by yearweek;

/* Create new table with yearweek and month year with another column identifying duplicates */
create table duplicates_yearmonth as select yearweek, yearmonth, row_number() over (partition by yearweek order by yearweek) as duplicates
from monthyear_map;

/* Delete duplicate rows */
delete from duplicates_yearmonth where duplicates = 2;

/* Drop the column which identifies duplicate now they have been removed */
alter table duplicates_yearmonth 
drop column duplicates;

/* Drop original table so that it can be replaced in the next query */
drop table monthyear_map;

/* Rename table with no duplicates the original name */
alter table duplicates_yearmonth 
rename to monthyear_map;

/* Count the number of service requests be yearmonths */
select yearmonth, count(*) 
from fact_service_quality as f
join aha199_mbis623.monthyear_map as m
on f.yearweek = m.yearweek
group by yearmonth
order by yearmonth;

/* --------------4. Provide the output for 3c in absence of the data warehouse---------------- */
/* Complete query which counts total number of requests per month using original nyc311 data without data warehouse */
select to_varchar("Created_Date", 'yyyy-MM') as yearmonth, count(*)
from nyc311.service_request_all
group by yearmonth
order by yearmonth;


