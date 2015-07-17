alter procedure [dbo].[CMSR_visitor](@start_date date, @end_Date date, @feature_id int, @FVT int ,@xday int, @pages varchar(100) )
as 
	--No row counts
	SET NOCOUNT ON;

	--No locks
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	
	
/* For debugging is needed 
declare @start_date date, @end_Date date, @feature_id int, @FVT int ,@xday int, @pages varchar(100)
set @start_Date = '2015-05-01' 
set @end_date = '2015-05-03'
set @feature_id = 6221 
set @xday = 7 
 set @pages = NULL 
 set @fvt = 1  */ 

	
/* This is the sproc to pull the visitor level information for a given feature id along with start and stop dates. The sproc has 4 parts 
Step1: Pull the subsession level information 
Step2: Filter any page constraints as needed (only if the user gave that as an input) 
Step3: Consolidate the subsession level data into visitor level data 
Step4: Write the visitor level data into permamanet tables (only for rows that satisfy certain condition) and pull some additional description from other tables
*/ 	
	
declare @time1 datetime ,  @cnt int ,  @id int 

set @time1 = GETDATE()
	
select @id = max(id) from msr_analytics..CMSR_sproc_job

insert into msr_analytics..CMSR_sproc_job (id,feature_id,startdate, enddate, FVT, xday, Time_subsession, time_pagespots_filters, time_visitor_table,  time_table_creation , completed, pages) values (@id+1,@feature_id, @start_date, @end_date, @FVT, @xday, 0,0,0,0,0, @pages)

update msr_analytics..CMSR_sproc_job 
set feature_id = @feature_id , startdate = @start_date , enddate = @end_Date, FVT = @FVT, xday = @xday
where ID = (@id + 1 ) 


--Step1: Pull the aggregate information (key Subsession id) 

---- create the table shell 
create table #temp_subsession_level 
(
feature_value_id int, 
feature_id int, 
fv_is_control int, 
fv_is_alt_control int, 
subsession_id bigint, 
session_id bigint, 
real_visitor_id bigint, 
fv_date smalldatetime, 
fv_count int, 
device_type_id tinyint, 
SE_exclusion_type int
)

---- Create actual data for FVT 

if @FVT = 1 
begin 

/* For debugging is needed. NOt to be used when running it as a sproc 
declare @start_date date, @end_Date date, @feature_id int, @FVT int ,@xday int, @pages varchar(100)
set @start_Date = '2015-05-01' ,set @end_date = '2015-05-30',set @feature_id = 6221 ,set @xday = 7 , set @pages = NULL,  set @fvt = 1 
 */ 

insert into #temp_subsession_level 
select  /* Aggregation is mainly done for fv_count */
	  max(r.feature_value_id) as feature_value_id
	, max(v.feature_id) as feature_id
	, max(case when v.value = '0' and v.feature_value_type_id <> 3 then 1 else 0 end) as fv_is_control
	, max(case when v.feature_value_type_id = 3 then 1 else 0 end) as fv_is_alt_control
	, s.tracking_id as subsession_id
	, max(se.session_id) as session_id
	, max(se.real_visitor_id) as real_visitor_id
	  /* Because of duplicates in std_test_treatment_tracking */
	, min(t.created) as fv_date
	, count(distinct r.feature_value_id) as fv_count -- Number of distinct feature values in one subsession
	, max(se.device_type_id) as device_type_id
	, max(case when se.exclusion_type_id > 0 then 1 else 0 end) as SE_exclusion_type -- will use it when session exclusion type is not used in where clause 
from griffin.grf.rtd_test_treatment_tracking r
join reporting.dbo.std_test_treatment_tracking s
  on r.treatment_tracking_id = s.treatment_tracking_id
 and isnull(s.permanent_treatment, 0) = 0-- Don't consider permanent treatments
 and s.recorded_date between @start_date and @end_Date
/* Get information on features. Won't break granularity */  
join reporting..sdd_feature_value v
  on r.feature_value_id = v.feature_value_id
/* Get real_visitor_id from grf.session_ex. Won't break granularity */
join griffin.pub.subsession ss
  on ss.subsession_id = s.tracking_id
join griffin.pub.session_ex se
  on se.session_id = ss.session_id
 --and se.exclusion_type_id = 0  ---- you are only removing sessions that are not valid. Not removing the entire visitor who has this exclusion type id. Rethink this move. 
 and se.real_visitor_id <> 0
/* Get fv touched information. Won't break granularity */
inner join reporting.dbo.std_test_feature_value_touched t
       on t.treatment_tracking_id = r.treatment_tracking_id
	 and t.feature_value_id = v.feature_id -- std_test_feature_value_touched.feature_value_id is misnamed. should be feature_id
where s.target_type_id = 1 -- Visitor
  and s.tracking_type_id = 2 -- Subsession
  and v.feature_id =  @feature_id 
group by s.tracking_id; -- 1 min

end 

---- Create actual data for FVT = 0 
if @FVT = 0
begin 

insert into #temp_subsession_level 
select /* Aggregation is mainly done for fv_count */
	  max(r.feature_value_id) as feature_value_id
	, max(v.feature_id) as feature_id
	, max(case when v.value = '0' and v.feature_value_type_id <> 3 then 1 else 0 end) as fv_is_control
	, max(case when v.feature_value_type_id = 3 then 1 else 0 end) as fv_is_alt_control
	, s.tracking_id as subsession_id
	, max(se.session_id) as session_id
	, max(se.real_visitor_id) as real_visitor_id
	  /* Because of duplicates in std_test_treatment_tracking */
	, min( s.recorded_date) as fv_date
	, count(distinct r.feature_value_id) as fv_count -- Number of distinct feature values in one subsession. Happens very rarely. 
	, max(se.device_type_id) as device_type_id
	, max(case when se.exclusion_type_id > 0 then 1 else 0 end) as SE_exclusion_type -- will use it when session exclusion type is not used in where clause 
from griffin.grf.rtd_test_treatment_tracking r
join reporting.dbo.std_test_treatment_tracking s
  on r.treatment_tracking_id = s.treatment_tracking_id
 and isnull(s.permanent_treatment, 0) = 0-- Don't consider permanent treatments
 and s.recorded_date between @start_date and @end_Date
/* Get information on features. Won't break granularity */  
join reporting..sdd_feature_value v
  on r.feature_value_id = v.feature_value_id
/* Get real_visitor_id from grf.session_ex. Won't break granularity */
join griffin.pub.subsession ss
  on ss.subsession_id = s.tracking_id
join griffin.pub.session_ex se
  on se.session_id = ss.session_id
 --and se.exclusion_type_id = 0 -- you are only removing sessions that are not valid. Not removing the entire visitor who has this exclusion type id. Rethink this move. 
 and se.real_visitor_id <> 0
/* Get fv touched information. Won't break granularity */
where s.target_type_id = 1 -- Visitor
  and s.tracking_type_id = 2 -- Subsession
  and v.feature_id =  @feature_id 
group by s.tracking_id; -- 1 min

end 
---- update time it took to run this step 
update msr_analytics..CMSR_sproc_job 
set subsession_cnt  = (select count(*) from #temp_subsession_level)
where ID = (@id + 1 ) 

update msr_analytics..CMSR_sproc_job 
set Time_subsession = datediff(mi, @time1, getdate())
where ID = (@id + 1 ) 


set @time1 = GETDATE()

-- Step 2: Delete only sessions that hit page spots as specified in the output 


if LEN(@pages) > 1 
	begin 
	
    declare @min_id bigint, @max_id bigint, @str varchar(1000) 

    create table #data (minid bigint)

    select @str = 'select top 1 navigation_id from spotlights_repl..vp_navigation where time_stamp between ''' + convert(varchar,@start_date) + ''' and ''' + convert(varchar,@start_date) + ' 00:02:00''' 
    insert #data exec(@str)
    select @min_id = minid from #data
    select @str = 'select top 1 navigation_id from spotlights_repl..vp_navigation where time_stamp between ''' + convert(varchar,@end_date) + ''' and ''' + convert(varchar,@end_date) + ' 00:02:00''' 
    insert #data exec(@str)
    select @max_id = max(minid) from #data

	set @str = ('delete
	from #temp_subsession_level
	where session_id not in
	(
	 select distinct t.session_id
	 from #temp_subsession_level t
	 join spotlights_repl..vp_navigation n
	 on n.session_id = t.session_id
	 and n.page_spot_id in ( ' +@pages + ' ) 
	 and navigation_id between ' + convert(varchar,@min_id) + ' and ' + convert(varchar,@max_id) + ')')
	 
	exec(@str)
	 
	drop table #data 

	end 	

update msr_analytics..CMSR_sproc_job 
set after_page_filtering_cnt = (select count(*) from #temp_subsession_level)
where ID = (@id + 1 ) 
	
update msr_analytics..CMSR_sproc_job 
set Time_pagespots_filters = datediff(mi, @time1, getdate())
where ID = (@id + 1 ) 	

set @time1 = GETDATE()


-- Step 3 : Produce Visitor level table (key:real_visitor id) 

--- Step 3.1 : Add row number to the subsession definition 
---- In the future, we want to find the first subsession where they touched the page or had the FVT fire - instead of picking the first subsession that were assigned to experiement and also had FVT or page touch
/* select a.* , ROW_NUMBER () over(partition by real_visitor_id order by fv_date) as Row_n  -- This logic is ok for FVT , FVA but not for FVA with page spots 
into #temp_subsession_level_mod
from #temp_subsession_level a */ 

---- Step 3.2 : Pull the visitor level table 


create table #temp_visitor_level 
(
test_type_id   int,
visitor_id   bigint NULL,
shopper_key   int NULL,
is_new bit, 
partner_program_promo_id   int,
website_country   varchar(2),
channel_group varchar(32), 
channel   varchar(20),
region   varchar(20),
initial_device_type_id   tinyint,
used_device_type varchar(6), 
test_id   int,
test_sub_id   int,
control_test   varchar(2),
is_filtered_out   int,
first_subsession_id   bigint, 
touch_date  smalldatetime, 
record_update_stamp   datetime,
session_exclusion_type   int
) 


insert into #temp_visitor_level 
select --top 0 
1 as Test_type_id 
     , f.real_visitor_id as visitor_id 
     , m.shopper_key
     , e.is_new
     , ss.partner_program_promo_id
     , p.website_country
	 , c.channel_group
	 , c.channel 
	 , rc.region 
	 , isnull(e.device_type_id, 0) as initial_device_type_id
	 , case when m.uses_SmallMobile = 1 and uses_Tablet = 0 then 'Mobile'
	       when m.uses_SmallMobile = 0 and uses_Tablet = 1 then 'Tablet'
	       when m.uses_SmallMobile = 1 and uses_Tablet = 1 then 'Both'
	       else 'None' end as used_device_types   
	 , f.feature_id as test_id 
	 , f.feature_value_id as test_sub_id 
     , case when f.fv_is_control = 1 then 'C' 
            when f.fv_is_alt_control = 1 then 'AC' 
            else 'T' end as control_test 
	, m.is_filtered_out
	, f.subsession_id as first_subsession_id
	, f.fv_date as touch_date 
	, GETDATE() as record_update_stamp 
	, m.session_exclusion_type 
from #temp_subsession_level f                                
join
(
	/* KEY: real_visitor_id , find hoppers at visitor level */
	select m_sub.real_visitor_id 
	    , min(m_sub.subsession_id) as min_fv_subsession_id
		, max(s.shopper_key) as shopper_key -- Data cleansing from grf.shopper (one visitor_id maps to more than one shopper_key's)
		, max(case when m_sub.device_type_id = 2 then 1 else 0 end) as uses_SmallMobile
		, max(case when m_sub.device_type_id = 3 then 1 else 0 end) as uses_Tablet
		, max(m_sub.session_exclusion_type) as session_exclusion_type
		, case when max(session_hopper) in (3,2) then max(session_hopper)
		       when count(distinct m_sub.feature_value_id) > 1 then 1 
		           else 0 end as is_filtered_out    
	from (--Key session id, first aggregate at session level and find hoppers 
	      select f.session_id , f.real_visitor_id 
		  /* Find out when visitor first was assigned or touched the feature */
		 , min(f.subsession_id) as subsession_id
		 , max(f.se_exclusion_type) as session_exclusion_type
		  /* Find sessions that was assigned or touched both control and test of the feature */
		 , case when max(f.fv_count) > 1                    then 3 
		       when count(distinct f.feature_value_id) > 1 then 2 
		       else 0 end as session_hopper 
		 , max(f.feature_value_id) as feature_value_id       
		 , max(device_type_id) as device_type_id
		 from #temp_subsession_level f
		 group by f.session_id , f.real_visitor_id  ) as m_sub  
		 left join griffin.pub.shopper s   on s.visitor_id = m_sub.real_visitor_id         
	group by m_sub.real_visitor_id
) m
  on f.real_visitor_id = m.real_visitor_id
 and f.subsession_id = m.min_fv_subsession_id -- After join key is real_visitor_id because #visitor_subsession_treatment_tracking keys off subsession_id
 --and f.fv_is_alt_control = 0 -- Only include control and test feature values
/* Get first subsessoin/session information. Won't break granularity. */
join griffin.pub.subsession ss
  on ss.subsession_id = f.subsession_id
join griffin.pub.session_ex e
  on e.session_id = ss.session_id
 and e.exclusion_type_id = 0
join griffin.pub.p3x p
  on p.partner_program_promo_id = ss.partner_program_promo_id
inner join griffin.pub.channel c on c.channel_id = p.channel_id 
inner join griffin.pub.website_country_region rc on rc.website_country = p.website_country 


update msr_analytics..CMSR_sproc_job 
set visitor_count = (select count(*) from #temp_visitor_level )
where ID = (@id + 1 ) 

update msr_analytics..CMSR_sproc_job 
set Time_visitor_table = datediff(mi, @time1, getdate())
where ID = (@id + 1 ) 	

set @time1 = GETDATE()

-- Step 4 : Write the values to a permanent table 

DECLARE @sql varchar(1000)


SET @sql = 'exec drop_table ''msr_analytics..MSR_VL_1_'+ convert(varchar,@feature_id) +''''

exec(@sql)

SET @sql = '
select test_type_id ,  visitor_id, shopper_key, is_new, partner_program_promo_id , website_country, channel_group, channel, region
, dt.device_type as initial_device_type, used_device_type, test_id, test_sub_id, control_test 
, f.description as test_name
, fv.description  as test_sub_name
, 1 as exposed_to_test
,is_filtered_out as is_hopper,  first_subsession_id,  touch_date,  record_update_stamp
into msr_analytics..MSR_VL_1_'+ convert(varchar,@feature_id)+ '  
from #temp_visitor_level vl 
inner join reporting..sdd_feature f on f.feature_id = vl.test_id 
inner join reporting..sdd_feature_value fv on fv.feature_value_id = vl.test_sub_id 
left join griffin.olap.dim_session_device_type dt on dt.device_type_id = vl.initial_device_type_id 
where session_exclusion_type = 0 
' 

exec(@sql)

update msr_analytics..CMSR_sproc_job 
set Final_count = (select count(*) from #temp_visitor_level where session_exclusion_type = 0 )
where ID = (@id + 1 ) 

update msr_analytics..CMSR_sproc_job 
set Time_table_creation = datediff(mi, @time1, getdate()), completed = 1 
where ID = (@id + 1 ) 

go 
