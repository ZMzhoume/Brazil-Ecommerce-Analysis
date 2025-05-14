-- 各市客户数分布
select
	customer_city,
		count(customer_id)
from
	olist_customers_dataset ocd
group by
	customer_city;
-- 各州客户数
select
	customer_state,
	count(customer_id)
from
	olist_customers_dataset ocd
group by
	customer_state;
-- 每个人购买力，因为每个订单都有唯一的客户ID，因此我们想要唯一的客户id进行求购买力
select
	customer_unique_id,
	sum(p_sum)
from
	(
	select
		customer_id,
		sum(payment_value) p_sum
	from
		ord_fulfillment_timeline oft ,
		payment p
	where
		oft.order_id = p.order_id
	group by
		customer_id) cusum
join olist_customers_dataset ocd on
	cusum.customer_id = ocd.customer_id
group by
	customer_unique_id ;
-- 每个州的购买力
select
	customer_state,
	round(sum(payment_value), 2) as 消费力
from
	payment
join ord_fulfillment_timeline oft on
	payment.order_id = oft.order_id
join olist_customers_dataset ocd on
	oft.customer_id = ocd.customer_id
group by
	customer_state
order by
	消费力 desc;
-- 每市的购买力
select
	customer_city ,
	round(sum(payment_value), 2) as 消费力
from
	payment
join ord_fulfillment_timeline oft on
	payment.order_id = oft.order_id
join olist_customers_dataset ocd on
	oft.customer_id = ocd.customer_id
group by
	customer_city
order by
	消费力 desc;
-- 客户的付款偏好
select
	payment_type,
	count(order_id)
from
	payment p
group by
	payment_type
order by
	count(order_id) desc;
-- 客戶購物時間偏好（周）
select
	dayofweek(oft.order_purchase_timestamp) as week_day,
	count(oft.order_id) 下单人数,
	round(sum(p.payment_value), 2) gmv
from
	ord_fulfillment_timeline oft
join payment p on
	oft.order_id = p.order_id
group by
	week_day
order by
	week_day;
-- 客戶購物時間偏好（hour）
select
	hour(oft.order_purchase_timestamp) as dayhour,
	count(oft.order_id) 下单人数,
	round(sum(p.payment_value), 2) gmv
from
	ord_fulfillment_timeline oft
join payment p on
	oft.order_id = p.order_id
group by
	dayhour
order by
	dayhour;
-- gmv，先提取所有的信息
create table order_time as
select
	oft.order_id,
	oft.customer_id,
	ocd.customer_unique_id,
	year(order_purchase_timestamp) y,
	month(order_purchase_timestamp) m,
	dayofweek(order_purchase_timestamp) da,
	date(order_purchase_timestamp) de,
	hour(order_purchase_timestamp) h,
	quarter(order_purchase_timestamp) q,
	oft.order_purchase_timestamp as opt
from
	ord_fulfillment_timeline oft 
join olist_customers_dataset ocd on
	oft.customer_id = ocd.customer_id;

desc order_time;

create table order_details as
select
	o.order_id,
	o.customer_id,
	o.customer_unique_id,
	o.y,
	o.q,
	o.m,
	o.da,
	o.de,
	o.h,
	o.opt,
	osm.total_cost
from
	order_time o
join order_summary osm on
	o.order_id = osm.order_id;
select * from order_details od ;
-- 季度jmv
select
	y,
	q,
	round(sum(total_cost)) gmv
from
	order_details
group by
	y,
	q
order by
	y,
	q;
-- 月jmv
select
	y,
	m,
	round(sum(total_cost),2) gmv
from
	order_details
group by
	y,
	m
order by
	y,
	m;
-- 计算arpu平均消费水平 季度
select y,q,round(sum(total_cost)/count(customer_unique_id),2) arpu
from order_details od 
group by y,q 
order by y,q;
-- arpu 月
select y,m ,round(sum(total_cost)/count(customer_unique_id),2) arpu
from order_details od 
group by y,m 
order by y,m ;
-- arpu day
select y,de ,round(sum(total_cost)/count(customer_unique_id),2) arpu
from order_details od 
group by y,de 
order by y,de;
-- 使用RFM模型对用户分类
-- f 购物次数频率
with rfm as(
select
	customer_unique_id,
	count(1) f,
	datediff(curdate(), max(de)) as t,
	round(sum(total_cost), 2) m,
	datediff(max(de),min(de))/365.0 as nian_p
from
	order_details od
group by
	customer_unique_id)
	,rfm_scores as (
select
	customer_unique_id,
	(case
		when f = 1 then 1
		when (f /nian_p) <= 1.5 then 2
		when (f /nian_p) <= 2.5 then 3
		when (f /nian_p) <= 3 then 4
		else 5
	end) as f_scores,
	ntile(5) over(order by t asc) as r_scores,  -- t已提前计算好的Recency值
        ntile(5) over(order by m asc) as m_scores    -- m已提前计算好的Monetary值
    from
        rfm
),rfm_fenceng as(
select customer_unique_id,
r_scores,
f_scores,
m_scores,
concat(r_scores,'-',f_scores,'-',m_scores) rfm_s,
CASE 
    WHEN r_scores >=4 AND f_scores >=4 AND m_scores >=4 THEN '重要价值客户'
    WHEN r_scores <=2 AND (f_scores <=2 OR m_scores <=2) THEN '流失风险客户'
    ELSE '一般客户'
  END AS rfm_segment
from rfm_scores)
select rfm_segment,
concat(round(count(rfm_segment)/(select count(1) from rfm_scores),2)*100,'%')
from rfm_fenceng
group by rfm_segment;

-- 废弃方案
create view Frequency as
select
	customer_unique_id,
	count(1) f,
	(case
		when count(1)= 1 then 1
		when (count(1)/((datediff(max(de),min(de)))/365.0)) <= 1.5 then 2
		when (count(1)/((datediff(max(de),min(de)))/365.0)) <= 2.5 then 3
		when (count(1)/((datediff(max(de),min(de)))/365.0)) <= 3 then 4
		else 5
	end) as f_scores
from
	order_details od
group by
	customer_unique_id ;
-- 最后消费时间
create view Recency as
select customer_unique_id,
datediff(curdate(),max(de)) as t, 
ntile(5) over(order by datediff(curdate(),max(de)) asc) as r_scores
from order_details od 
group by customer_unique_id;
-- select r_scores,count(1) from recency
-- group by r_scores
-- 消费金额
create view Monetary as 
select customer_unique_id,
round(sum(total_cost),2),
ntile(5) over(order by round(sum(total_cost),2)) as m_scores
from order_details od 
group by customer_unique_id;
-- 基于rfm模型对客户进行分层
create view rfm1 as
select recency.customer_unique_id,
r_scores,
f_scores,
m_scores,
concat(r_scores,'-',f_scores,'-',m_scores) rfm_s,
CASE 
    WHEN r_scores >=4 AND f_scores >=4 AND m_scores >=4 THEN '重要价值客户'
    WHEN r_scores <=2 AND (f_scores <=2 OR m_scores <=2) THEN '流失风险客户'
    ELSE '一般客户'
    end as rfm_agnt
 from recency join frequency on recency.customer_unique_id = frequency.customer_unique_id
join Monetary on recency.customer_unique_id=Monetary.customer_unique_id;
select * from rfm1