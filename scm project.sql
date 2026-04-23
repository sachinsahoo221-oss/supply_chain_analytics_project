use supply_chain_analytics_project;

select * from orders;
select* from shipments;
select * from products;
select * from rto;


-- 📊 Performance Metrics

with timely_delivery as
(select 
round(sum(case when actual_delivery<= expected_delivery then 1 else 0 end) *100.0/ count(*),2) On_time_delivery_percent
from shipments),
lead_time as
(select round(avg(datediff(actual_delivery,dispatch_date)),2) avg_lead_time
from shipments),
returns as
(select round(count(distinct r.order_id) *100.0/ count(distinct o.order_id),2) as rto_percentage
from orders o 
left join rto r
on r.order_id = o.order_id),
total_count as
(select count(o.order_id) total_orders,
count(s.shipment_id) total_shipments
from orders o 
left join shipments s
on s.order_id = o.order_id)

select tc.total_orders,
tc.total_shipments,
td.On_time_delivery_percent,
lt.avg_lead_time,
r.rto_percentage
from timely_delivery td
cross join lead_time lt
cross join returns r
cross join total_count tc;

--- 🚚 Courier Analysis

 select courier_name, -- Which courier has the best on-time performance
 round(sum(case when actual_delivery <= expected_delivery then 1 else 0 end) *100.0/count(*),2) as on_time_performance 
 from shipments
 group by courier_name
 order by on_time_performance 
 desc limit 1; 
 
 select courier_name, -- Which courier causes maximum delays
 round(avg(datediff(actual_delivery,expected_delivery)),2) as Highest_avg_delivery_days 
 from shipments
 where actual_delivery > expected_delivery
 group by courier_name 
 order by Highest_avg_delivery_days desc 
 limit 1; 
 
 select * from -- Top 3 worst-performing couriers
 (select courier_name,total_orders,delayed_orders, delayed_percentage, 
 row_number() over(order by delayed_percentage desc) rnk 
 from (select courier_name, 
 count(*) total_orders,
 sum(case when actual_delivery > expected_delivery then 1 else 0 end) delayed_orders,
 round(sum(case when actual_delivery > expected_delivery then 1 else 0 end) *100.0 / count(*),2) delayed_percentage
 from shipments 
 group by courier_name) a )
 t where rnk <= 3;
 
 
 --- 🌍 Location Analysis
 
select o.city, -- Which city has the highest delivery delay?
count(o.order_id) total_order,
round(avg(datediff(s.actual_delivery, s.expected_delivery)),2) Highest_avg_delivery_days 
from orders o 
join shipments s 
   on s.order_id = o.order_id 
   where actual_delivery > expected_delivery
group by o.city 
order by Highest_avg_delivery_days desc
limit 1;

select city, -- Which city has the highest order volume?
count(order_id) order_volume
from orders 
group by city 
order by order_volume desc 
limit 1;

select o.city, -- City-wise RTO %
 count(o.order_id) order_volume, count(r.order_id) rto_count,
 round(count(distinct r.order_id) *100/ count(distinct o.order_id),2) as RTO_by_city 
 from orders o 
 Left join rto r 
 on o.order_id = r.order_id 
 group by o.city 
 order by RTO_by_city desc;
 
 --- 📦 Product & Demand 
 
 with demand as -- Top 5 most demanded products
 (select o.product_id, p.product_name,p.category, 
 sum(o.quantity) total_demand 
 from products p
 join orders o 
   on o.product_id = p.product_id
group by o.product_id,p.product_name,p.category),
 rnkk as 
 (select *, rank() over(order by total_demand desc) rnk 
 from demand)
 select * from rnkk where rnk <= 5;
 
 with demand as -- Category-wise demand
 (select p.category,
 sum(o.quantity) total_demand 
 from products p 
 join orders o 
 on o.product_id = p.product_id 
 group by p.category) 
 select *, 
 dense_rank() over(order by total_demand desc) rnk
 from demand;
 
 select -- Monthly demand trend
 product_id,
 year(order_date) yr, month(order_date) month_num, sum(quantity) total_demand,
 rank() over(partition by year(order_date), month(order_date) order by sum(quantity) desc) rnk
 from orders
 group by product_id, year(order_date) , month(order_date) ;
 
 --- ⏱️ Delay Analysis

select -- Average delay (in days)
round(avg(datediff(actual_delivery,expected_delivery)),2) avg_delayed_days
from shipments 
where actual_delivery > expected_delivery;

select -- % of delayed shipments
round(sum(case when actual_delivery > expected_delivery then 1 else 0 end) *100
/ count(shipment_id),2) as delayed_shipment
from shipments;

select -- Orders delayed by more than 3 days
 count(order_id) order_delayed_by_3_days
from shipments 
where datediff(actual_delivery, expected_delivery) >= 3;

--- 🔄 RTO Deep Dive

select -- top reasons for RTO
return_reason, count(order_id) total_rto_count
from rto
group by return_reason
order by total_rto_count desc;

select -- Which city has highest RTO and why
o.city, 
count(distinct o.order_id) total_orders,
count( distinct r.order_id) rto_count,
round(count(distinct r.order_id) *100.0 / count(distinct o.order_id),2) as rto_percentage,
rank() over(order by round(count(distinct r.order_id) *100.0 / count(distinct o.order_id),2)  desc) rnk 
from orders o
left join rto r
on r.order_id = o.order_id 
group by o.city;

with rto_reason as
(select o.city, r.return_reason, count(r.order_id) total_rto
from orders o 
join rto r
   on r.order_id = o.order_id
group by o.city, r.return_reason
order by total_rto desc)
select * from rto_reason 
where city = 'Hyderabad';

with courier_rto as
(SELECT o.city, s.courier_name,
COUNT(r.order_id) AS rto_orders
FROM orders o
JOIN shipments s ON o.order_id = s.order_id
JOIN rto r ON o.order_id = r.order_id
GROUP BY o.city, s.courier_name
ORDER BY rto_orders DESC)
select * from courier_rto 
where city = 'Hyderabad';

with product_rto as
(select o.city, o.product_id,p.product_name, count(*) total_rto_product
from orders o 
join rto r
  on r.order_id = o.order_id
join products p
  on p.product_id = o.product_id
group by o.city, o.product_id,p.product_name
order by total_rto_product desc)
select * from product_rto
where city = 'Hyderabad';

with delay as
(select o.city, avg(datediff(s.actual_delivery,s.expected_delivery)) avg_delay,
count(r.order_id) total_rto
from orders o 
join shipments s
  on s.order_id = o.order_id
left join rto r
  on r.order_id = o.order_id
  where s.actual_delivery > s.expected_delivery
group by o.city
order by total_rto desc)
select * from delay
 where city = 'Hyderabad';

select  -- Courier vs RTO relationship
o.city, avg(datediff(s.actual_delivery,s.expected_delivery)) avg_delay,
count(r.order_id) total_rto
from orders o 
join shipments s
  on s.order_id = o.order_id
left join rto r
  on r.order_id = o.order_id
  where s.actual_delivery > s.expected_delivery
group by o.city
order by total_rto desc;


--- 🏭 Supplier Analysis

select   -- Highest lead time 
s.supplier_name, p.category,
round(avg(datediff(s.actual_delivery, s.dispatch_date)),2) as lead_time,
rank() over( order by round(avg(datediff(s.actual_delivery, s.dispatch_date)),2)  desc) rnk
from shipments s
join orders o 
  on o.order_id = s.order_id
join products p
on p.product_id = o.product_id
group by s.supplier_name, p.category;

select -- Supplier-wise delivery performance
supplier_name, round(avg(datediff(actual_delivery, dispatch_date)),2) as lead_time
from shipments
group  by supplier_name
order by lead_time desc;

select -- supplier_scorecard
supplier_name,
count(*) as total_order_delivered,
round(sum(case when actual_delivery <= expected_delivery then 1 else 0 end) *100.0 /count(*),2) On_time_delivery_percentage,
round(sum(case when actual_delivery > expected_delivery then 1 else 0 end) *100.0/ count(*),2) Delayed_percentage
from shipments 
group by supplier_name
order by On_time_delivery_percentage desc;


with supplier_s as -- Delayed supplier by city
(select o.city,s.supplier_name, 
round(sum(case when s.actual_delivery > s.expected_delivery then 1 else 0 end) *100.0/ count(*),2) Delayed_percentage
from shipments s
join orders o
on o.order_id = s.order_id
group by s.supplier_name, o.city
order by Delayed_percentage desc),
  rnkk as
 (select *, 
rank() over(partition by city order by Delayed_percentage desc) rnk
from supplier_s)
select * from rnkk where rnk = 1;



--- 🔍 Root Cause Analysis

with courier_delay as
(select courier_name, count(*) total_orders,
sum(case when actual_delivery > expected_delivery then 1 else 0 end) Highest_delayed_orders,
round(sum(case when actual_delivery > expected_delivery then 1 else 0 end) *100.0 /count(*),2) Highest_delayed_percentage
from shipments
group by courier_name
order by Highest_delayed_orders desc
limit 1),
supplier_delay as
(select supplier_name, count(*) total_orders,
round(avg(datediff(actual_delivery,expected_delivery)),2) Highest_avg_delayed_days
from shipments
where actual_delivery > expected_delivery
group by supplier_name
order by Highest_avg_delayed_days desc
limit 1),

city_delay as
(select o.city, count(*) total_orders,
round(avg(datediff(s.actual_delivery, s.dispatch_date)),2) Highest_avg_lead_time
from orders o 
join shipments s
on o.order_id = s.order_id
group by o.city
order by Highest_avg_lead_time desc
limit 1)

select cd.courier_name,cd.total_orders,cd.Highest_delayed_orders,cd.Highest_delayed_percentage,
sd.supplier_name, sd.total_orders, sd.Highest_avg_delayed_days,
c.city, c.total_orders, c.Highest_avg_lead_time

from courier_delay cd
cross join supplier_delay sd
cross join city_delay c
;

--- 📈 Trend Analysis

select  -- Monthly on-time delivery trend  
  year(actual_delivery) yr, month(actual_delivery) month_num, count(order_id) total_orders
  from shipments
  where actual_delivery <= expected_delivery
  group by year(actual_delivery) , month(actual_delivery)
  order by yr,month_num;
  
with total_orders as   -- Running total of orders
(select  
  year(order_date) yr, month(order_date) month_num, count(order_id) total_orders
  from orders
  group by year(order_date) , month(order_date))
  select *, 
  sum(total_orders) over(order by yr, month_num) running_total
  from total_orders;


with spike as    -- Demand spike detection
(select product_id ,
year(order_date) yr,
month(order_date) month_num,
sum(quantity) total_demand
from orders
group by product_id , year(order_date) , month(order_date)),
demand as
(select *, 
lag(total_demand) over(partition by product_id order by yr, month_num) prev_month_demand,
total_demand -lag(total_demand) over(partition by product_id order by yr, month_num) demand_spike
from spike)

select * from demand
where total_demand > prev_month_demand *1.5;


--- ⚠️ Risk Detection
select * from orders;
select* from shipments;
select * from products;
select * from rto;


select o.product_id,   --  Which products are at risk of delay
round(avg(datediff(s.actual_delivery, s.expected_delivery)),2) avg_delayed_days
from orders o
join shipments s
on s.order_id = o.order_id
where s.actual_delivery > s.expected_delivery
group by o.product_id
order by avg_delayed_days desc;

 --- Which courier is consistently underperforming?
WITH courier_performance AS (
    SELECT 
        courier_name,
        YEAR(actual_delivery) AS yr,
        MONTH(actual_delivery) AS mn,
        (YEAR(actual_delivery) * 12 + MONTH(actual_delivery)) AS month_seq,
        ROUND(
            SUM(CASE WHEN actual_delivery > expected_delivery THEN 1 ELSE 0 END) * 100.0 
            / COUNT(*), 2
        ) AS delayed_percentage
    FROM shipments
    GROUP BY courier_name, YEAR(actual_delivery), MONTH(actual_delivery)
),

bad_months AS (
    SELECT 
        courier_name,
        (YEAR(actual_delivery) * 12 + MONTH(actual_delivery)) AS month_seq,
        ROW_NUMBER() OVER (
            PARTITION BY courier_name 
            ORDER BY YEAR(actual_delivery), MONTH(actual_delivery)
        ) AS rn
    FROM shipments
    WHERE actual_delivery > expected_delivery
),

streak_base AS (
    SELECT 
        courier_name,
        month_seq,
        month_seq - rn AS grp
    FROM bad_months
),

final_calc AS (
    SELECT 
        courier_name,
        grp,
        MIN(month_seq) AS start_seq,
        MAX(month_seq) AS end_seq,
        COUNT(*) AS consecutive_bad_months
    FROM streak_base
    GROUP BY courier_name, grp
)

SELECT *
FROM final_calc
WHERE consecutive_bad_months >= 3
ORDER BY consecutive_bad_months DESC;
 
 --- Top 3 products in each city with highest delays

 with city_delay as
(select o.city, o.product_id, count(*) total_orders,
round(avg(datediff(s.actual_delivery,s.expected_delivery)),2) as highest_delays
from orders o
join shipments s
on s.order_id = o.order_id
where s.actual_delivery > s.expected_delivery
group by o.city, o.product_id),
rnkk as
(select *,
dense_rank() over(partition by city order by highest_delays desc) rnk
from city_delay) 

select * from rnkk
where rnk <= 3;
