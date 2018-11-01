#!/bin/bash
source /home/dongruan/.bashrc
sys_time=`date "+%Y-%m-%d %H:%M:%S"` 
echo "[HIVE-EXEC-MONTH]$sys_time-----------------------------------------------------------------"

#标准时间
lower=`date -d "1 days ago" +'%Y-%m-%d %H:%M:%S'` 
#操作日期
ctime3=`date -d "$lower" +'%Y-%m-%d'`
#月份操作起始日期
opmonth1=`date -d "$lower" +'%Y-%m-01'`
#月份操作当前日期
opmonth2=`date -d "$lower" +'%Y-%m-%d'`
#归属月份
opmonth3=`date -d "$lower" +'%Y%m'`

metric_time_start=`date "+%Y-%m-%d %H:%M:%S"`

echo "[PARAM]:lower($lower)ctime3($ctime3)-opmonth1($opmonth1)opmonth2($opmonth2)opmonth3($opmonth3)"

hive -hiveconf tez.queue.name="dongruanQ5" -e"
use dongruanbase;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.exec.dynamic.partition.mode=nonstrict;

TRUNCATE TABLE RPT_HUMTRF_REGION_FROM_MON_TEMP;
TRUNCATE TABLE RPT_HUMTRF_REGION_FROM_MON;
TRUNCATE TABLE RPT_HUMTRF_REGION_FROM_MON_SQOOP;


-- 月报告 出行方式月汇总
INSERT INTO TABLE RPT_HUMTRF_REGION_TORUIST_USER_DAY_MID PARTITION(stat_date)
SELECT 
         x.MSISDN,
         '-1',
         '-1',
         x.CITY_CODE,
         x.CITY_NAME,
         '-1',
         '合计',
         x.VEHICLE_TYPE,
         '${ctime3}' AS STAT_DATE
FROM(
     SELECT MSISDN,
             CITY_CODE,
             CITY_NAME,
             VEHICLE_TYPE,
             stat_date,
             rank() over(partition BY MSISDN ORDER BY stat_date ASC ,VEHICLE_TYPE DESC) AS rank_num
      FROM
            RPT_HUMTRF_REGION_TORUIST_USER_DAY_MID 
      WHERE 
             stat_date >= '${opmonth1}' and stat_date <= '${opmonth2}'
      group by 
             MSISDN,
             CITY_CODE, 
             CITY_NAME,
             VEHICLE_TYPE,
             stat_date 
)x  where x.rank_num=1  
GROUP BY 
     x.MSISDN,
         x.CITY_CODE,
         x.CITY_NAME,
         '-1',
         '合计',
         x.VEHICLE_TYPE;

-- 区域人数/人次临时表统计
INSERT INTO TABLE rpt_humtrf_region_from_mon_temp
SELECT '${opmonth3}' AS stat_mon,
       '${ctime3}' AS stat_date,
       a.analyse_region_code AS region_code,
       a.analyse_region_name AS region_name,
       a.analyse_county_code AS county_code,
       a.analyse_county_name AS county_name,
       a.analyse_city_code AS city_code,
       a.analyse_city_name AS city_name,
       a.home_city_code AS home_area_code,
       a.home_city_name AS home_area_name,
       '1' AS in_prov_flag,
       count(a.msisdn) AS user_visit_count_total
FROM RPT_HUMTRF_REGION_FROM_DAY_MID a
WHERE a.stat_date>='${opmonth1}' AND a.stat_date<='${opmonth2}'
  AND a.home_province_code='1006'
GROUP BY 
         '${opmonth3}',
         '${ctime3}',
         a.analyse_region_code,
         a.analyse_region_name,
         a.analyse_county_code,
         a.analyse_county_name,
         a.analyse_city_code,
         a.analyse_city_name,
         a.home_city_code,
         a.home_city_name;


INSERT INTO TABLE rpt_humtrf_region_from_mon_temp
SELECT '${opmonth3}' AS stat_mon,
       '${ctime3}' AS stat_date,
       a.analyse_region_code AS region_code,
       a.analyse_region_name AS region_name,
       a.analyse_county_code AS county_code,
       a.analyse_county_name AS county_name,
       a.analyse_city_code AS city_code,
       a.analyse_city_name AS city_name,
       a.home_province_code AS home_area_code,
       a.home_province_name AS home_area_name,
       '2' AS in_prov_flag,
       count(a.msisdn) AS user_visit_count_total
FROM RPT_HUMTRF_REGION_FROM_DAY_MID a
WHERE a.stat_date>='${opmonth1}' AND a.stat_date<='${opmonth2}'
  AND a.home_province_code!='1006'
GROUP BY 
         '${opmonth3}',
         '${ctime3}',
         a.analyse_region_code,
         a.analyse_region_name,
         a.analyse_county_code,
         a.analyse_county_name,
         a.analyse_city_code,
         a.analyse_city_name,
         a.home_province_code,
         a.home_province_name;


INSERT INTO TABLE rpt_humtrf_region_from_mon_temp
SELECT '${opmonth3}' AS stat_mon,
       '${ctime3}' AS stat_date,
       a.analyse_region_code AS region_code,
       a.analyse_region_name AS region_name,
       a.analyse_county_code AS county_code,
       a.analyse_county_name AS county_name,
       a.analyse_city_code AS city_code,
       a.analyse_city_name AS city_name,
       a.home_country_code AS home_area_code,
       a.home_country_name AS home_area_name,
       '4' AS in_prov_flag,
       count(a.msisdn) AS user_visit_count_total
FROM RPT_HUMTRF_REGION_FROM_DAY_MID a
WHERE a.stat_date>='${opmonth1}' AND a.stat_date<='${opmonth2}'
  AND a.home_country_code!='460'
  AND a.home_country_code!='461'
GROUP BY 
        '${opmonth3}',
         '${ctime3}',
         a.analyse_region_code,
         a.analyse_region_name,
         a.analyse_county_code,
         a.analyse_county_name,
         a.analyse_city_code,
         a.analyse_city_name,
         a.home_country_code,
         a.home_country_name;




-- 对丽江古城景区进行整体数据汇总 (省内其他州市游客)
INSERT INTO TABLE RPT_HUMTRF_REGION_FROM_MON 
SELECT
   d.stat_mon            AS stat_mon,
   a.analyse_region_code AS region_code,
   a.analyse_region_name AS region_name,
   a.analyse_county_code AS county_code,
   a.analyse_county_name AS county_name,
   a.analyse_city_code   AS city_code,
   a.analyse_city_name   AS city_name,
   '1' as in_prov_flag,
   a.home_city_code  AS home_area_code,
   a.home_city_name  AS home_area_name,
   count(a.msisdn)*1.8 as user_number_count_total,
   d.user_visit_count_total*1.8 as user_visit_count_total,
   -- 出行方式
   SUM(
     CASE
         WHEN c.vehicle_type=1 THEN 1 ELSE 0 
     END
   )*1.8 AS bus_user_cnt,
   SUM(
     CASE
         WHEN c.vehicle_type=2 THEN 1 ELSE 0 
     END
   )*1.8 AS train_user_cnt,
   SUM(
     CASE
         WHEN c.vehicle_type=3 THEN 1 ELSE 0 
     END
   )*1.8 AS fly_user_cnt,
   SUM(
     CASE
         WHEN c.vehicle_type < 1 OR c.vehicle_type > 3  OR c.vehicle_type is null THEN 1 ELSE 0
     END
   )*1.8 AS other_user_cnt,
   -- 性别
   SUM(
     CASE
         WHEN a.gender = 2 THEN 1 ELSE 0 
     END
   )*1.8 AS sex_woman_cnt,
   SUM(
     CASE
         WHEN a.gender = 1 THEN 1 ELSE 0 
     END
   )*1.8 AS sex_man_cnt,
   SUM(
     CASE
         WHEN a.gender not in ('1','2') or a.gender is null THEN 1 ELSE 0 
     END
   )*1.8 AS sex_other_cnt,
   -- 年龄 age
   SUM(
     CASE
         WHEN a.age<18 THEN 1 ELSE 0
     END
   )*1.8 AS age1_user_cnt,
   SUM(
     CASE
         WHEN a.age >= 18 AND a.age < 22 THEN 1 ELSE 0
     END
   )*1.8 AS age2_user_cnt,
   SUM(
     CASE
         WHEN a.age >= 22 AND a.age < 28 THEN 1 ELSE 0
     END
   )*1.8 AS age3_user_cnt,
   SUM(
     CASE
         WHEN a.age >= 28 AND a.age < 35 THEN 1 ELSE 0
     END
   )*1.8 AS age4_user_cnt,
   SUM(
     CASE
         WHEN a.age >= 35 AND a.age < 50 THEN 1 ELSE 0
     END
   )*1.8 AS age5_user_cnt,
   SUM(
     CASE
         WHEN a.age >= 50 THEN 1 ELSE 0
     END
   )*1.8 AS age6_user_cnt,
   SUM(
     CASE
         WHEN  a.age IS NULL THEN 1 ELSE 0
     END
   )*1.8 AS age9_user_cnt

FROM(
    SELECT 
       m.msisdn,
       m.analyse_region_code,
       m.analyse_region_name,
       m.analyse_county_code,
       m.analyse_county_name,
       m.analyse_city_code,
       m.analyse_city_name,
       m.home_county_code,
       m.home_county_name,
       m.home_city_code,
       m.home_city_name,
       u.gender,
       u.age
     FROM 
       RPT_HUMTRF_REGION_FROM_DAY_MID m
     LEFT JOIN
       TS_USER_BSI_USER_INFO u ON m.msisdn=u.msisdn 
     WHERE
       m.stat_date>='${opmonth1}' AND m.stat_date<='${opmonth2}' and m.home_province_code ='1006' AND m.analyse_city_code='0888' AND m.analyse_county_code='P0PA'
     GROUP BY  
       m.msisdn,
       m.analyse_region_code,
       m.analyse_region_name,
       m.analyse_county_code,
       m.analyse_county_name,
       m.analyse_city_code,
       m.analyse_city_name,
       m.home_county_code,
       m.home_county_name,
       m.home_city_code,
       m.home_city_name,
       u.gender,
       u.age
) a
LEFT OUTER JOIN
   RPT_HUMTRF_REGION_TORUIST_USER_DAY_MID c 
   ON 
      a.msisdn = c.msisdn and c.PROVINCE_CODE='-1' and c.stat_date='${ctime3}'
LEFT OUTER JOIN
   rpt_humtrf_region_from_mon_temp d 
   ON  
     d.region_code = a.analyse_region_code
   AND
     d.county_code = a.analyse_county_code
   AND
     d.city_code = a.analyse_city_code
   AND 
     d.home_area_code = a.home_city_code
   AND 
     d.in_prov_flag='1'
   AND 
     d.stat_date='${ctime3}'
  
GROUP BY 
   d.stat_mon,
   a.analyse_region_code,
   a.analyse_region_name,
   a.analyse_county_code,
   a.analyse_county_name,
   a.analyse_city_code,
   a.analyse_city_name,
   a.home_city_code,
   a.home_city_name,
   d.user_visit_count_total  ;



-- 对丽江古城景区进行整体数据汇总(外省游客)
INSERT INTO TABLE RPT_HUMTRF_REGION_FROM_MON 
SELECT
   d.stat_mon            AS stat_mon,
   a.analyse_region_code AS region_code,
   a.analyse_region_name AS region_name,
   a.analyse_county_code AS county_code,
   a.analyse_county_name AS county_name,
   a.analyse_city_code   AS city_code,
   a.analyse_city_name   AS city_name,
   '2' as in_prov_flag,
   a.home_province_code  AS home_area_code,
   a.home_province_name  AS home_area_name,
   count(a.msisdn)*1.8 as user_number_count_total,
   d.user_visit_count_total*1.8 as user_visit_count_total,
   -- 出行方式
   SUM(
     CASE
         WHEN c.vehicle_type=1 THEN 1 ELSE 0 
     END
   )*1.8 AS bus_user_cnt,
   SUM(
     CASE
         WHEN c.vehicle_type=2 THEN 1 ELSE 0 
     END
   )*1.8 AS train_user_cnt,
   SUM(
     CASE
         WHEN c.vehicle_type=3 THEN 1 ELSE 0 
     END
   )*1.8 AS fly_user_cnt,
   SUM(
     CASE
         WHEN c.vehicle_type < 1 OR c.vehicle_type > 3  OR c.vehicle_type is null THEN 1 ELSE 0
     END
   )*1.8 AS other_user_cnt,
   -- 性别
   '-1' AS sex_woman_cnt,
   '-1' AS sex_man_cnt,
   '-1' AS sex_other_cnt,
   -- 年龄 age
   '-1' AS age1_user_cnt,
   '-1' AS age2_user_cnt,
   '-1' AS age3_user_cnt,
   '-1' AS age4_user_cnt,
   '-1' AS age5_user_cnt,
   '-1' AS age6_user_cnt,
   '-1' AS age9_user_cnt
FROM(
    SELECT 
       m.msisdn,
       m.analyse_region_code,
       m.analyse_region_name,
       m.analyse_county_code,
       m.analyse_county_name,
       m.analyse_city_code,
       m.analyse_city_name,
       m.home_province_code,
       m.home_province_name
     FROM 
       RPT_HUMTRF_REGION_FROM_DAY_MID m
     WHERE
       m.stat_date>='${opmonth1}' AND m.stat_date<='${opmonth2}' AND m.home_province_code!='1006' AND m.analyse_city_code='0888' AND m.analyse_county_code='P0PA'
     GROUP BY  
       m.msisdn,
       m.analyse_region_code,
       m.analyse_region_name,
       m.analyse_county_code,
       m.analyse_county_name,
       m.analyse_city_code,
       m.analyse_city_name,
       m.home_province_code,
       m.home_province_name
) a
LEFT OUTER JOIN
   RPT_HUMTRF_REGION_TORUIST_USER_DAY_MID c 
   ON 
      a.msisdn = c.msisdn  and c.PROVINCE_CODE='-1' and  c.stat_date='${ctime3}'
LEFT OUTER JOIN
   rpt_humtrf_region_from_mon_temp d 
   ON  
     d.region_code = a.analyse_region_code
   AND
     d.county_code = a.analyse_county_code
   AND
     d.city_code = a.analyse_city_code
   AND 
     d.home_area_code = a.home_province_code
   AND 
     d.in_prov_flag='2'
   AND 
     d.stat_date='${ctime3}'
GROUP BY 
   d.stat_mon,
   a.analyse_region_code,
   a.analyse_region_name,
   a.analyse_county_code,
   a.analyse_county_name,
   a.analyse_city_code,
   a.analyse_city_name,
   a.home_province_code,
   a.home_province_name,
   d.user_visit_count_total ;


---对丽江古城景区进行整体数据汇总(国际游客)
INSERT INTO TABLE RPT_HUMTRF_REGION_FROM_MON 
SELECT
   d.stat_mon            AS stat_mon,
   a.analyse_region_code AS region_code,
   a.analyse_region_name AS region_name,
   a.analyse_county_code AS county_code,
   a.analyse_county_name AS county_name,
   a.analyse_city_code   AS city_code,
   a.analyse_city_name   AS city_name,
   '4' as in_prov_flag,
   a.home_country_code  AS home_area_code,
   a.home_country_name  AS home_area_name,
   count(a.msisdn)*1.8 as user_number_count_total,
   d.user_visit_count_total*1.8 as user_visit_count_total,
   -- 出行方式
   '-1' AS bus_user_cnt,
   '-1' AS train_user_cnt,
   '-1' AS fly_user_cnt,
   '-1' AS other_user_cnt,
   -- 性别
   '-1' AS sex_woman_cnt,
   '-1' AS sex_man_cnt,
   '-1' AS sex_other_cnt,
   -- 年龄 age
   '-1' AS age1_user_cnt,
   '-1' AS age2_user_cnt,
   '-1' AS age3_user_cnt,
   '-1' AS age4_user_cnt,
   '-1' AS age5_user_cnt,
   '-1' AS age6_user_cnt,
   '-1' AS age9_user_cnt
FROM(
    SELECT 
       m.msisdn,
       m.analyse_region_code,
       m.analyse_region_name,
       m.analyse_county_code,
       m.analyse_county_name,
       m.analyse_city_code,
       m.analyse_city_name,
       m.home_country_code,
       m.home_country_name
     FROM 
       RPT_HUMTRF_REGION_FROM_DAY_MID m
     WHERE
       m.stat_date>='${opmonth1}' AND m.stat_date<='${opmonth2}' AND m.home_country_code!='460' AND m.home_country_code!='461'
     GROUP BY  
       m.msisdn,
       m.analyse_region_code,
       m.analyse_region_name,
       m.analyse_county_code,
       m.analyse_county_name,
       m.analyse_city_code,
       m.analyse_city_name,
       m.home_country_code,
       m.home_country_name
) a
LEFT OUTER JOIN
   RPT_HUMTRF_REGION_TORUIST_USER_DAY_MID c 
   ON 
      a.msisdn = c.msisdn and c.PROVINCE_CODE='-1' and c.stat_date='${ctime3}'
LEFT OUTER JOIN
   rpt_humtrf_region_from_mon_temp d 
   ON  
     d.region_code = a.analyse_region_code
   AND
     d.county_code = a.analyse_county_code
   AND
     d.city_code = a.analyse_city_code
   AND 
     d.home_area_code = a.home_country_code
   AND 
     d.in_prov_flag='4'
   AND 
     d.stat_date='${ctime3}'
GROUP BY 
   d.stat_mon,
   a.analyse_region_code,
   a.analyse_region_name,
   a.analyse_county_code,
   a.analyse_county_name,
   a.analyse_city_code,
   a.analyse_city_name,
   a.home_country_code,
   a.home_country_name,
   d.user_visit_count_total ;

-- 合计统计
INSERT INTO TABLE RPT_HUMTRF_REGION_FROM_MON
SELECT a.stat_mon AS stat_mon,
       a.region_code AS region_code,
       a.region_name AS region_name,
       a.county_code AS county_code,
       a.county_name AS county_name,
       a.city_code AS city_code,
       a.city_name AS city_name,
       '1' AS in_prov_flag,
       '-1' AS home_area_code,
       '合计' AS home_area_name,
       SUM(a.user_number_count_total) AS user_number_count_total,
       SUM(a.user_visit_count_total) AS user_visit_count_total,
       SUM(a.bus_user_cnt)   AS bus_user_cnt,
       SUM(a.train_user_cnt) AS train_user_cnt,
       SUM(a.fly_user_cnt)   AS fly_user_cnt,
       SUM(a.other_user_cnt) AS other_user_cnt,
       SUM(a.sex_woman_cnt)  AS sex_woman_cnt,
       SUM(a.sex_man_cnt)   AS sex_man_cnt,
       SUM(a.sex_other_cnt) AS sex_other_cnt,
       SUM(a.age1_user_cnt) AS age1_user_cnt,
       SUM(a.age2_user_cnt) AS age2_user_cnt,
       SUM(a.age3_user_cnt) AS age3_user_cnt,
       SUM(a.age4_user_cnt) AS age4_user_cnt,
       SUM(a.age5_user_cnt) AS age5_user_cnt,
       SUM(a.age6_user_cnt) AS age6_user_cnt,
       SUM(a.age9_user_cnt) AS age9_user_cnt
       
FROM RPT_HUMTRF_REGION_FROM_MON a
WHERE a.stat_mon='${opmonth3}' AND a.in_prov_flag='1' AND a.home_area_code!='-1'
GROUP BY a.stat_mon ,
         a.region_code,
         a.region_name,
         a.county_code,
         a.county_name,
         a.city_code,
         a.city_name;

INSERT INTO TABLE RPT_HUMTRF_REGION_FROM_MON
SELECT a.stat_mon AS stat_mon,
       a.region_code AS region_code,
       a.region_name AS region_name,
       a.county_code AS county_code,
       a.county_name AS county_name,
       a.city_code AS city_code,
       a.city_name AS city_name,
       '2'         AS in_prov_flag,
       '-1'        AS home_area_code,
       '合计'       AS home_area_name,
       SUM(a.user_number_count_total) AS user_number_count_total,
       SUM(a.user_visit_count_total) AS user_visit_count_total,
       SUM(a.bus_user_cnt)   AS bus_user_cnt,
       SUM(a.train_user_cnt) AS train_user_cnt,
       SUM(a.fly_user_cnt)   AS fly_user_cnt,
       SUM(a.other_user_cnt) AS other_user_cnt,
       '-1' AS sex_woman_cnt,
       '-1' AS sex_man_cnt,
       '-1' AS sex_other_cnt,
       '-1' AS age1_user_cnt,
       '-1' AS age2_user_cnt,
       '-1' AS age3_user_cnt,
       '-1' AS age4_user_cnt,
       '-1' AS age5_user_cnt,
       '-1' AS age6_user_cnt,
       '-1' AS age9_user_cnt
       
FROM RPT_HUMTRF_REGION_FROM_MON a
WHERE a.stat_mon='${opmonth3}' AND a.in_prov_flag='2' AND a.home_area_code!='-1'
GROUP BY a.stat_mon ,
         a.region_code,
         a.region_name,
         a.county_code,
         a.county_name,
         a.city_code,
         a.city_name;

INSERT INTO TABLE RPT_HUMTRF_REGION_FROM_MON
SELECT a.stat_mon AS stat_mon,
       a.region_code AS region_code,
       a.region_name AS region_name,
       a.county_code AS county_code,
       a.county_name AS county_name,
       a.city_code AS city_code,
       a.city_name AS city_name,
       '4'         AS in_prov_flag,
       '-1'        AS home_area_code,
       '合计'      AS home_area_name,
       SUM(a.user_number_count_total) AS user_number_count_total,
       SUM(a.user_visit_count_total)  AS user_visit_count_total,
       '-1' AS bus_user_cnt,
       '-1' AS train_user_cnt,
       '-1' AS fly_user_cnt,
       '-1' AS other_user_cnt,
       '-1' AS sex_woman_cnt,
       '-1' AS sex_man_cnt,
       '-1' AS sex_other_cnt,
       '-1' AS age1_user_cnt,
       '-1' AS age2_user_cnt,
       '-1' AS age3_user_cnt,
       '-1' AS age4_user_cnt,
       '-1' AS age5_user_cnt,
       '-1' AS age6_user_cnt,
       '-1' AS age9_user_cnt
FROM RPT_HUMTRF_REGION_FROM_MON a
WHERE a.stat_mon='${opmonth3}' AND a.in_prov_flag='4' AND a.home_area_code!='-1'
GROUP BY a.stat_mon ,
         a.region_code,
         a.region_name,
         a.county_code,
         a.county_name,
         a.city_code,
         a.city_name;

INSERT INTO TABLE RPT_HUMTRF_REGION_FROM_MON_SQOOP
SELECT *
FROM RPT_HUMTRF_REGION_FROM_MON
WHERE stat_mon='${opmonth3}';

"

metric_time_end=`date "+%Y-%m-%d %H:%M:%S"`
start_seconds=$(date --date="$metric_time_start" +%s);
end_seconds=$(date --date="$metric_time_end" +%s);
used_seconds=$((end_seconds-start_seconds))
echo "[EXEC]:system_time($sys_time)start_time($metric_time_start)end_time($metric_time_end)used_time($used_seconds)s"
