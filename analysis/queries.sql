
--Number of Trips everyday
select tpep_pickup_datetime::date as pickup_date,count(*) from
fact_trips
group by pickup_date
order by 1

--Total Sales by payment_type
select payment_id,payment_method,floor(sum(total_amount))
from
fact_trips 
left join dim_payment dp on dp.payment_id = fact_trips.payment_type
where total_amount>0
group by 1,2

--Finding the longest journey on each day
with MaxJourneys as (
select
	tpep_pickup_datetime::date as pickup_date,
	pu.zone as Pickup_Zone,
	df.zone as Dropoff_Zone,
	ft.journeyminutes,
	row_number() over (partition by tpep_pickup_datetime::date order by ft.journeyminutes desc) as ranked
from
	fact_trips ft
left join
	dim_zone pu on pu.locationid = ft."PULocationID"
left join 
	dim_zone df on df.locationid= ft."DOLocationID"
group by 1,2,3,4
)
select * from MaxJourneys 
where ranked=1
