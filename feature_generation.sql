USE ROLE IC_ENG_ROLE;

---- need input as the user_base_table
---- need intermediate_table_type; db; schema
set user_base_table = 'sandbox_db.dianedou.user_base_table_demo';

create or replace {intermediate_table_type} table {db}.{schema}.base_features as

WITH week_ref as (select min(wk_ref) as wk_begin, max(wk_ref) as wk_end from table($user_base_table))


, fuga as (  --- save running time from querying fuga
  select fuga.* from  dwh.fact_user_growth_accounting fuga
  join week_ref on fuga.full_date_pt >= dateadd ('day', -1, wk_begin)
  and  fuga.full_date_pt <= dateadd ('day', 6, wk_end))

select mut.user_id
        ,mut.variant
        ,mut.created_at_pt
        ,mut.wk_ref
        ,date_trunc(day, fuga.full_date_pt) as measured_date
        ,fuga.is_wao
        ,fuga.is_mao
        ,fuga.is_hao
        ,fuga.gtv_l1:overall::numeric(10,2) as gtv_l1
        ,fuga.gtv_l7:overall::numeric(10,2) as gtv_l7
        ,fuga.gtv_l28:overall::numeric(10,2) as gtv_l28
        ,fuga.gtv_l91:overall::numeric(10,2) as gtv_l91
        ,fuga.gtv_lifetime:overall::numeric(10,2) as gtv_lifetime
        ,fuga.deliveries_l1:overall::numeric(10,2) as deliveries_l1
        ,fuga.deliveries_l7:overall::numeric(10,2) as deliveries_l7
        ,fuga.deliveries_l28:overall::numeric(10,2) as deliveries_l28
        ,fuga.deliveries_lifetime:overall::numeric(10,2) as deliveries_lifetime
        ,fuga.visits_l1:overall::numeric(10,2) as visits_l1
        ,fuga.visits_l7:overall::numeric(10,2) as visits_l7
        ,fuga.visits_l28:overall::numeric(10,2) as visits_l28
        ,fuga.visits_l91:overall::numeric(10,2) as visits_l91
        ,fuga.visits_lifetime:overall::numeric(10,2) as visits_lifetime
        , ifnull(datediff('day',fuga.signup_date_pt, fuga.full_date_pt), -1) as signup_days
        , ifnull(datediff('day', fuga.activation_date_pt, fuga.full_date_pt), -1) as activation_days
        , ad.days_signup_to_activation
        , MAX(ifnull(ad.activation_warehouse_name, 'X')) as activation_warehouse_name
        , MAX(ifnull(ad.email_domain, 'X')) as email_domain
        , MAX(ifnull(ad.attrb_activation_channel, 'X')) as attrb_activation_channel
        , MAX(ifnull(ad.activation_platform, 'X')) as activation_platform
        , MAX(ifnull(ad.activation_region_id, -1)) as activation_region_name
from fuga
inner join table($user_base_table) mut on fuga.user_id = mut.user_id
and fuga.full_date_pt = dateadd('day',-1,mut.created_at_pt)
left join dwh.dim_activation AS ad ON mut.user_id = ad.user_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25;

--- get past express status of user_id before created_at_pt
create or replace table {intermediate_table_type} table {db}.{schema}.express_past_express_status as

  SELECT
    base.user_id,
    base.created_at_pt,
    max(cast(    (subscription_type = 'paid') and (term_length = 'year') as int) ) as past_annual,
    max(cast(   (subscription_type = 'paid') and (term_length in ('month','three months','six months')) as int) ) as past_monthly,
    max(cast(   (subscription_type = 'free')  as int) ) as past_trial
    from instadata.rds_data.subscriptions s
    join table($user_base_table) base
    on base.user_id = s.user_id
    and base.created_at_pt > CONVERT_TIMEZONE('UTC', 'US/Pacific', s.starts_on)
    group by 1,2
;

create or replace table {intermediate_table_type} table {db}.{schema}.shopping as

with visits_raw as (
select base.user_id,
    base.created_at_pt,
    datediff(hour,visit_start_date_time_pt, visit_end_date_time_pt) as visit_duration,
    platform,
    visit_start_date_time_pt
    from dwh.fact_user_visit fuv
    join table($user_base_table) base on fuv.user_id =base.user_id
    and fuv.visit_end_date_time_pt  >= dateadd(day,-90, base.created_at_pt)
    and fuv.visit_end_date_time_pt < base.created_at_pt
)
,

visits as (
select
    user_id,
    created_at_pt,
    min(  datediff(day, visit_start_date_time_pt,  created_at_pt)  ) as days_since_last_visit,
    sum( visit_duration ) as tot_visit_time,
    count( visit_duration ) as cnt_visits,
    count(distinct platform) as n_platforms_visited,
    max( case when platform in ('android' ,'ios') then 1 else 0 end) as ever_mobile,
    min(  least(28,datediff(day, visit_start_date_time_pt,  created_at_pt )  )) as days_since_last_visit_l28,
    sum( case when visit_start_date_time_pt >= dateadd(day,-28, created_at_pt) then  visit_duration  end ) as tot_visit_time_l28,
    count(  case when visit_start_date_time_pt >= dateadd(day,-28, created_at_pt) then  visit_duration end) as cnt_visits_l28,
    count(distinct  case when visit_start_date_time_pt >= dateadd(day,-28, created_at_pt) then  platform else null end ) as n_platforms_visited_l28,
    max( case when visit_start_date_time_pt >= dateadd(day,-28, created_at_pt) and platform in ('android' ,'ios') then 1 else 0 end) as ever_mobile_l28
    from visits_raw
    group by 1,2
)
,

del_raw as (
    select
    fod.user_id,
    base.created_at_pt,
    fod.delivery_type,
    order_id,
    delivery_created_date_time_pt,
    delivery_created_date_time_pt >= dateadd(day,-28, base.created_at_pt) as l28,
    order_delivery_gmv_amt_usd,
    tip_amt_usd,
    initial_tip_amt_usd,
    DATEDIFF(day, LAG(delivery_created_date_time_pt) OVER (PARTITION BY fod.user_id ORDER BY delivery_created_date_time_pt), delivery_created_date_time_pt)  AS days_between_reorder

    from dwh.fact_order_delivery AS fod
    join table($user_base_table) base on base.user_id = fod.user_id
    and fod.delivery_created_date_time_pt < base.created_at_pt
    where  fod.delivery_created_date_time_pt >= '2019-01-01'
    and fod.delivery_state = 'delivered'
),

del AS (
  SELECT
    user_id,
    created_at_pt,
    count(delivery_created_date_time_pt) as n_deliveries,
    min( datediff(day, delivery_created_date_time_pt, created_at_pt)  ) as days_since_last_completed_order,
    min( days_between_reorder ) AS min_days_between_reorder,
    SUM(order_delivery_gmv_amt_usd) AS gmv,
    SUM(tip_amt_usd)  AS tip,
    COALESCE(SUM(tip_amt_usd)/NULLIF(SUM(initial_tip_amt_usd), 0), 0) AS avg_change_tip_pct,
    SUM( case when tip_amt_usd = 0 then 1 else 0 end ) as n_tips
  FROM del_raw
    group by 1,2
),

del_28 AS (
  SELECT
    user_id,
    created_at_pt,
    count(delivery_created_date_time_pt) as n_deliveries_l28,
    min( datediff(day, delivery_created_date_time_pt, created_at_pt)  ) as days_since_last_completed_order_l28,
    min(days_between_reorder) AS min_days_between_reorder_l28,
    SUM(order_delivery_gmv_amt_usd) AS gmv_l28,
    SUM(tip_amt_usd) AS tip_l28,
    COALESCE(SUM(tip_amt_usd)/NULLIF(SUM(initial_tip_amt_usd), 0), 0) AS avg_change_tip_pct_l28,
    SUM( case when tip_amt_usd = 0 then 1 else 0 end ) as n_tips_l28,
    sum( case when delivery_type in ('priorty_eta','hyperfast','one_hour') then 1 end) as n_fast_delivery_l28,
    sum( case when delivery_type = 'pickup' then 1 end) as n_pickup_l28
  FROM del_raw where l28
    group by 1,2
)


SELECT
base.user_id,
base.created_at_pt,
visits.tot_visit_time as tot_visit_time,
visits.cnt_visits as cnt_visits,
visits.n_platforms_visited as n_platforms_visited,
visits.ever_mobile as ever_mobile,
visits.tot_visit_time_l28 as tot_visit_time_l28,
visits.cnt_visits_l28 as cnt_visits_l28,
visits.n_platforms_visited_l28 as n_platforms_visited_l28,
visits.ever_mobile_l28 as ever_mobile_l28,
del.days_since_last_completed_order as days_since_last_completed_order,
del.n_deliveries as n_deliveries,
del.min_days_between_reorder as min_days_between_reorder,
del.gmv as gmv,
del.tip as tip,
del.avg_change_tip_pct as avg_change_tip_pct,
del.n_tips as n_tips,
del_28.days_since_last_completed_order_l28 as days_since_last_completed_order_l28,
del_28.n_deliveries_l28 as n_deliveries_l28,
del_28.min_days_between_reorder_l28 as min_days_between_reorder_l28,
del_28.gmv_l28 as gmv_l28,
del_28.tip_l28 as tip_l28,
del_28.avg_change_tip_pct_l28 as avg_change_tip_pct_l28,
del_28.n_tips_l28 as n_tips_l28,
del_28.n_fast_delivery_l28 as n_fast_delivery_l28,
del_28.n_pickup_l28 as n_pickup_l28

FROM table($user_base_table) base
LEFT JOIN visits on base.user_id = visits.user_id and base.created_at_pt = visits.created_at_pt
LEFT JOIN del  on base.user_id = del.user_id and base.created_at_pt = del.created_at_pt
LEFT JOIN del_28 on base.user_id = del_28.user_id and base.created_at_pt = del_28.created_at_pt
;

CREATE OR REPLACE TABLE {intermediate_table_type} table {db}.{schema}.itemattributes AS
SELECT
    base.user_id,
    base.created_at_pt,
    caia.max_days_since_previous_order_fav_store_last_five_deliveries,
    caia.avg_days_since_previous_order_fav_store_last_five_deliveries,
    caia.days_since_last_order_fav_store_last_five_deliveries,
    caia.avg_initial_charge_amt_usd_fav_store_last_five_deliveries,
    caia.avg_initial_tip_amt_usd_fav_store_last_five_deliveries,
    caia.num_pickup_fav_store_last_five_deliveries,
    caia.num_deliveries_fav_store_last_five_deliveries,
    caia.total_items_fav_store_last_five_deliveries,
    caia.total_spend_fav_store_last_five_deliveries,
    caia.pct_alcohol_items_fav_store_last_five_deliveries,
    caia.pct_kosher_items_fav_store_last_five_deliveries,
    caia.pct_low_fat_items_fav_store_last_five_deliveries,
    caia.pct_organic_items_fav_store_last_five_deliveries,
    caia.pct_sugar_free_items_fav_store_last_five_deliveries,
    caia.pct_vegan_items_fav_store_last_five_deliveries,
    caia.pct_has_ingredient_items_fav_store_last_five_deliveries,
    caia.pct_fat_free_items_fav_store_last_five_deliveries,
    caia.pct_gluten_free_items_fav_store_last_five_deliveries,
    caia.pct_vegetarian_items_fav_store_last_five_deliveries,
    caia.pct_spend_alcohol_fav_store_last_five_deliveries,
    caia.pct_spend_kosher_fav_store_last_five_deliveries,
    caia.pct_spend_low_fat_fav_store_last_five_deliveries,
    caia.pct_spend_organic_fav_store_last_five_deliveries,
    caia.pct_spend_sugar_free_fav_store_last_five_deliveries,
    caia.pct_spend_vegan_fav_store_last_five_deliveries,
    caia.pct_spend_has_ingredient_fav_store_last_five_deliveries,
    caia.pct_spend_fat_free_fav_store_last_five_deliveries,
    caia.pct_spend_gluten_free_fav_store_last_five_deliveries,
    caia.pct_spend_vegetarian_fav_store_last_five_deliveries

FROM table($user_base_table) base
LEFT JOIN INSTADATA.ML.FEATURE__CUSTOMERS_AVG_ITEM_ATTRIBUTES caia
    ON base.user_id = caia.user_id
    AND caia.ds = dateadd('day', -1, base.created_at_pt)
;

CREATE OR REPLACE TABLE {intermediate_table_type} table {db}.{schema}.matching_attributes as

select
base.*,
Xp.past_annual as past_annual,
Xp.past_monthly as past_monthly,
Xp.past_trial as past_trial,
coalesce(tot_visit_time,0) as tot_visit_time,
coalesce(n_platforms_visited,0) as n_platforms_visited,
coalesce(ever_mobile,0) as ever_mobile,
coalesce(tot_visit_time_l28,0) as tot_visit_time_l28,
coalesce(cnt_visits_l28,0) as cnt_visits_l28,
coalesce(n_platforms_visited_l28,0) as n_platforms_visited_l28,
coalesce(ever_mobile_l28,0) as ever_mobile_l28,
coalesce(days_since_last_completed_order,0) as days_since_last_completed_order,
coalesce(min_days_between_reorder,0) as min_days_between_reorder,
coalesce(tip,0) as tip,
coalesce(avg_change_tip_pct,0) as avg_change_tip_pct,
coalesce(n_tips,0) as n_tips,
coalesce(days_since_last_completed_order_l28,0) as days_since_last_completed_order_l28,
coalesce(min_days_between_reorder_l28,0) as min_days_between_reorder_l28,
coalesce(tip_l28,0) as tip_l28,
coalesce(avg_change_tip_pct_l28,0) as avg_change_tip_pct_l28,
coalesce(n_tips_l28,0) as n_tips_l28,
coalesce(n_fast_delivery_l28,0) as n_fast_delivery_l28,
coalesce(n_pickup_l28,0) as n_pickup_l28,
coalesce(max_days_since_previous_order_fav_store_last_five_deliveries,0) as max_days_since_previous_order_fav_store_last_five_deliveries,
coalesce(avg_days_since_previous_order_fav_store_last_five_deliveries,0) as avg_days_since_previous_order_fav_store_last_five_deliveries,
coalesce(days_since_last_order_fav_store_last_five_deliveries,0) as days_since_last_order_fav_store_last_five_deliveries,
coalesce(avg_initial_charge_amt_usd_fav_store_last_five_deliveries,0) as avg_initial_charge_amt_usd_fav_store_last_five_deliveries,
coalesce(avg_initial_tip_amt_usd_fav_store_last_five_deliveries,0) as avg_initial_tip_amt_usd_fav_store_last_five_deliveries,
coalesce(num_pickup_fav_store_last_five_deliveries,0) as num_pickup_fav_store_last_five_deliveries,
coalesce(num_deliveries_fav_store_last_five_deliveries,0) as num_deliveries_fav_store_last_five_deliveries,
coalesce(total_items_fav_store_last_five_deliveries,0) as total_items_fav_store_last_five_deliveries,
coalesce(total_spend_fav_store_last_five_deliveries,0) as total_spend_fav_store_last_five_deliveries,
coalesce(pct_alcohol_items_fav_store_last_five_deliveries,0) as pct_alcohol_items_fav_store_last_five_deliveries,
coalesce(pct_kosher_items_fav_store_last_five_deliveries,0) as pct_kosher_items_fav_store_last_five_deliveries,
coalesce(pct_low_fat_items_fav_store_last_five_deliveries,0) as pct_low_fat_items_fav_store_last_five_deliveries,
coalesce(pct_organic_items_fav_store_last_five_deliveries,0) as pct_organic_items_fav_store_last_five_deliveries,
coalesce(pct_sugar_free_items_fav_store_last_five_deliveries,0) as pct_sugar_free_items_fav_store_last_five_deliveries,
coalesce(pct_vegan_items_fav_store_last_five_deliveries,0) as pct_vegan_items_fav_store_last_five_deliveries,
coalesce(pct_has_ingredient_items_fav_store_last_five_deliveries,0) as pct_has_ingredient_items_fav_store_last_five_deliveries,
coalesce(pct_fat_free_items_fav_store_last_five_deliveries,0) as pct_fat_free_items_fav_store_last_five_deliveries,
coalesce(pct_gluten_free_items_fav_store_last_five_deliveries,0) as pct_gluten_free_items_fav_store_last_five_deliveries,
coalesce(pct_vegetarian_items_fav_store_last_five_deliveries,0) as pct_vegetarian_items_fav_store_last_five_deliveries,
coalesce(pct_spend_alcohol_fav_store_last_five_deliveries,0) as pct_spend_alcohol_fav_store_last_five_deliveries,
coalesce(pct_spend_kosher_fav_store_last_five_deliveries,0) as pct_spend_kosher_fav_store_last_five_deliveries,
coalesce(pct_spend_low_fat_fav_store_last_five_deliveries,0) as pct_spend_low_fat_fav_store_last_five_deliveries,
coalesce(pct_spend_organic_fav_store_last_five_deliveries,0) as pct_spend_organic_fav_store_last_five_deliveries,
coalesce(pct_spend_sugar_free_fav_store_last_five_deliveries,0) as pct_spend_sugar_free_fav_store_last_five_deliveries,
coalesce(pct_spend_vegan_fav_store_last_five_deliveries,0) as pct_spend_vegan_fav_store_last_five_deliveries,
coalesce(pct_spend_has_ingredient_fav_store_last_five_deliveries,0) as pct_spend_has_ingredient_fav_store_last_five_deliveries,
coalesce(pct_spend_fat_free_fav_store_last_five_deliveries,0) as pct_spend_fat_free_fav_store_last_five_deliveries,
coalesce(pct_spend_gluten_free_fav_store_last_five_deliveries,0) as pct_spend_gluten_free_fav_store_last_five_deliveries,
coalesce(pct_spend_vegetarian_fav_store_last_five_deliveries,0) as pct_spend_vegetarian_fav_store_last_five_deliveries

from {intermediate_table_type} table {db}.{schema}.base_features base
left join {intermediate_table_type} table {db}.{schema}.shopping shop  on base.user_id = shop.user_id  and base.created_at_pt = shop.created_at_pt---and base.reference_month = shop.reference_month
left join {intermediate_table_type} table {db}.{schema}.itemattributes item  on base.user_id = item.user_id  and base.created_at_pt = item.created_at_pt--and base.reference_month = item.reference_month
left join {intermediate_table_type} table {db}.{schema}.express_past_express_status Xp on base.user_id = Xp.user_id and base.created_at_pt = Xp.created_at_pt--and base.reference_month = Xp.reference_month
;

CREATE OR REPLACE TABLE {intermediate_table_type} table {db}.{schema}.matching_attributes_with_reporting_metrics as

SELECT m.*
, fuga.is_mao as reporting_is_mao
, COALESCE(fuga.gtv_l28:overall,0) as reporting_gtv_l28
FROM sandbox_db.dianedou.matching_attributes m
left join dwh.fact_user_growth_accounting AS fuga
ON m.user_id = fuga.user_id
AND fuga.full_date_pt = '2022-10-23'
;
