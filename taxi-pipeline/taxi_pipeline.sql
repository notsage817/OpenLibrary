select min(trip_pickup_date_time), max(trip_pickup_date_time) 
from taxi_pipeline.taxi_data.taxi_records;

select avg(case when payment_type = 'Credit' then 1 else 0 end) as credit_card_percentage
from taxi_pipeline.taxi_data.taxi_records;

select sum(tip_amt) as total_tips
from taxi_pipeline.taxi_data.taxi_records;