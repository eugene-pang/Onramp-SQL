drop table if exists final_output;
create table final_output as 
with sum_table as (
select 
	  s.eventid
  	, e.venueid
  	, c.catname
  	, c.catid
  	, s.month
    , s.t_sales
  	, row_number() over(order by s.month, c.catid, e.venueid, s.t_sales desc) as row_id --For RT month
  	, sum(sum(t_sales)) over (partition by month) as sum_group_month --For RT
    , sum(sum(t_sales)) over(partition by month, c.catid) as sum_group_categories --For Ranking and RT
    , sum(sum(t_sales)) over(partition by month, c.catid, e.venueid) as sum_group_venue --For Ranking
    
  	, sum(sum(t_sales)) over(partition by month, e.venueid) as sum_group_venue_RT -- For RT
  	, sum(sum(t_sales)) over(partition by month, s.eventid) as sum_group_event_RT -- For RT
from (select 
      	  eventid
      	, date_part(month, saletime) as month
      	, sum(pricepaid) * sum(qtysold) as t_sales 
      from sales 
      group by eventid, date_part(month, saletime)) s
join event e
	on s.eventid = e.eventid
join category c
  	on e.catid = c.catid
group by s.eventid
  	, e.venueid
  	, c.catname
  	, c.catid
  	, s.month
    , s.t_sales
),

rank_table as (
select
	  *
    , dense_rank() over(partition by month order by sum_group_categories desc) as rank_categories --1 = in that month; that category was number 1 
    , dense_rank() over(partition by month, catid order by sum_group_venue desc) as rank_venue -- 1 = in that month and category; that venue is #1
    , dense_rank() over(partition by month, catid, venueid order by t_sales desc) as rank_event -- 1 = in that month, category, and venue; the event is #1 	
  	, row_number() over(order by month, sum_group_categories desc) as row_id_cat --need for % diff 
  	, row_number() over(order by venueid, month) as row_id_venue --For RT Venue
  	, row_number() over(order by eventid, month) as row_id_event --For RT Event
  	, row_number() over(order by catid, month) as row_id_cat_RT --For RT
from sum_table
order by row_id
),

Lag_table as (
select
	  *
   	, coalesce(lag(sum_group_month) over(order by row_id), 0) as lag_month --lag using rowid so in order of month
  	, coalesce(lag(sum_group_categories) over(order by row_id_cat), 1) as lag_cat --need for % diff  
  	, coalesce(lag(sum_group_venue_RT) over(order by row_id_venue), 0) as lag_venue_RT
  	, coalesce(lag(sum_group_event_RT) over(order by row_id_event), 0) as lag_event_RT
  	, coalesce(lag(sum_group_categories) over(order by row_id_cat_RT), 0) as lag_cat_RT
from rank_table
group by eventid, venueid, catname, catid, month, t_sales, row_id, sum_group_month, sum_group_categories, sum_group_venue 
    , rank_categories , rank_venue , rank_event , row_id_cat, row_id_venue, row_id_event, sum_group_venue_RT, sum_group_event_RT, row_id_cat_RT
),


RT_table as (
select
	  *
    , sum(case when lag_month != sum_group_month then sum_group_month else 0 end) 
    		over(order by row_id rows between unbounded preceding and current row) as RT_month -- same RT # if same month, ordered by month
    , sum(case when lag_venue_RT != sum_group_venue_RT then sum_group_venue_RT else 0 end)
    		over(partition by venueid order by row_id_venue rows between unbounded preceding and current row) as RT_venue
  	, sum(case when lag_event_RT != sum_group_event_RT then sum_group_event_RT else 0 end)
    		over(partition by eventid order by row_id_event rows between unbounded preceding and current row) as RT_event
  	, sum(case when lag_cat_RT != sum_group_categories then sum_group_categories else 0 end)
    		over(partition by catid order by row_id_cat_RT rows between unbounded preceding and current row) as RT_categories
  	, case
  		when rank_categories = 1 then 0
  		else first_value((lag_cat - sum_group_categories) / lag_cat * 100) over(partition by month, catid) * -1
  		end as per_diff_categories
from Lag_table
group by eventid, venueid, catname, catid, month, t_sales, row_id, sum_group_month, sum_group_categories, sum_group_venue 
    , rank_categories , rank_venue , rank_event , row_id_cat, row_id_venue, row_id_event, sum_group_venue_RT, sum_group_event_RT, row_id_cat_RT
  	, lag_month, lag_cat, lag_venue_RT, lag_event_RT, lag_cat_RT
)

select 
	  eventid
  	, venueid
  	, catname
  	, catid
  	, month
    , t_sales
    , rank_categories
    , rank_venue
    , rank_event
    , RT_month
    , RT_categories
    , RT_event
    , RT_venue
    , per_diff_categories
from RT_table
order by month, rank_categories, rank_venue, rank_event
;
    
