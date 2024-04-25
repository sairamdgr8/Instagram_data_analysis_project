
create db saidb;

create or replace TABLE SAIDB.PUBLIC.FOLLOWERS_TBL (
	ID NUMBER(38,0) autoincrement start 1 increment 1 noorder,
	FOLLOWERS VARCHAR(1000)
);



create database saidb;

create table followers_tbl (
  id integer autoincrement, -- auto incrementing IDs
  followers varchar (1000)  -- variable string column
  
);
show tables 
select * from followers_tbl

-- delete from followers_tbl


create table following_tbl (
  id integer autoincrement, -- auto incrementing IDs
  following_ varchar (1000)  -- variable string column
  
);

select * from following_tbl

select count(*) from followers_tbl
select count(*) from following_tbl

create table web_activity_tbl (
		
  id integer autoincrement, -- auto incrementing IDs
  name varchar(2000),
  events_count varchar (1000)  -- variable string column
  
);


create table logged_past_insights_reels_tbl (
		
  id integer autoincrement, -- auto incrementing IDs
  tag_ varchar(2000) -- variable string column
  
);


create table media_reels_tbl (
		
  id integer autoincrement, -- auto incrementing IDs
  total_media_reels_count integer  -- integer string column
  
);

create table media_stories_tbl (
		
  id integer autoincrement, -- auto incrementing IDs
  total_media_stories_count integer  -- integer string column
  
);

create table message_inbox_activity_tbl (
		
  id integer autoincrement, -- auto incrementing IDs
  total_message_inbox_activity_count integer  -- integer string column
  
);

create table audience_insights_city_tbl (
		
  id integer autoincrement, -- auto incrementing IDs
  city varchar(2000),
  "percentage%" DECIMAL(20,10)  -- decimal string column
  
);

select * from audience_insights_city_tbl

create table audience_insights_country_tbl (
		
  id integer autoincrement, -- auto incrementing IDs
  country varchar(2000),
  "percentage%" DECIMAL(20,10)  -- decimal string column
  
);

select * from audience_insights_country_tbl


create table audience_insights_age_tbl (
		
  id integer autoincrement, -- auto incrementing IDs
  age varchar(2000),
  "percentage%" DECIMAL(20,10)  -- decimal string column
  
);

select * from audience_insights_age_tbl


create table audience_insights_men_tbl (
		
  id integer autoincrement, -- auto incrementing IDs
  men varchar(2000),
  "percentage%" DECIMAL(20,10)  -- decimal string column
  
);

select * from audience_insights_men_tbl

create table audience_insights_women_tbl (
		
  id integer autoincrement, -- auto incrementing IDs
  women varchar(2000),
  "percentage%" DECIMAL(20,10)  -- decimal string column
  
);

select * from audience_insights_women_tbl



create table liked_comments_tbl (
		
  id integer autoincrement, -- auto incrementing IDs
  liked_comments varchar(2000)
  
);

select * from liked_comments_tbl;


create table liked_posts_tbl (
		
  id integer autoincrement, -- auto incrementing IDs
  liked_posts varchar(2000)
  
);

select * from liked_posts_tbl;

create table ads_viewed_tbl (

  id integer autoincrement, -- auto incrementing IDs
  ads_viewed varchar(2000)

);

select * from ads_viewed_tbl;

