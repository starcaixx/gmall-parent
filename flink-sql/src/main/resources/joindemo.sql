-- -- 开启 mini-batch
-- SET table.exec.mini-batch.enabled=true;
-- -- mini-batch的时间间隔，即作业需要额外忍受的延迟
-- SET table.exec.mini-batch.allow-latency=1s;
-- -- 一个 mini-batch 中允许最多缓存的数据
-- SET table.exec.mini-batch.size=1000;
-- -- 开启 local-global 优化
-- SET table.optimizer.agg-phase-strategy=TWO_PHASE;
--
-- -- 开启 distinct agg 切分
-- SET table.optimizer.distinct-agg.split.enabled=true;


-- source
CREATE TABLE finbalashort (
ISVALID int,
CREATETIME TIMESTAMP(6),
UPDATETIME TIMESTAMP(3),
COM_UNI_CODE bigint,
END_DATE TIMESTAMP(6),
CURY_TYPE_PAR int,
CURY_UNIT_PAR int,
BS_11001 decimal(24,8),
BS_11002 decimal(24,8),
BS_11003 decimal(24,8),
BS_11000 decimal(24,8),
BS_12001 decimal(24,8),
BS_10000 decimal(24,8),
BS_21001 decimal(24,8),
BS_21002 decimal(24,8),
BS_21000 decimal(24,8),
BS_22000 decimal(24,8),
BS_20000 decimal(24,8),
BS_30001 decimal(24,8),
BS_30002 decimal(24,8),
BS_30003 decimal(24,8),
BS_30004 decimal(24,8),
BS_31000 decimal(24,8),
BS_32000 decimal(24,8),
BS_30000 decimal(24,8),
BS_11016 decimal(24,8),
BS_11031 decimal(24,8),
BS_11067 decimal(24,8),
BS_11070 decimal(24,8),
BS_11079 decimal(24,8),
BS_11082 decimal(24,8),
BS_12013 decimal(24,8),
BS_12016 decimal(24,8),
BS_12019 decimal(24,8),
BS_12022 decimal(24,8),
BS_12025 decimal(24,8),
BS_12031 decimal(24,8),
BS_12034 decimal(24,8),
BS_12037 decimal(24,8),
BS_12040 decimal(24,8),
BS_12043 decimal(24,8),
BS_12046 decimal(24,8),
BS_12049 decimal(24,8),
BS_12052 decimal(24,8),
BS_12055 decimal(24,8),
BS_12058 decimal(24,8),
BS_12064 decimal(24,8),
BS_12000 decimal(24,8),
BS_21003 decimal(24,8),
BS_21019 decimal(24,8),
BS_21070 decimal(24,8),
BS_21079 decimal(24,8),
BS_21082 decimal(24,8),
BS_21085 decimal(24,8),
BS_21088 decimal(24,8),
BS_21091 decimal(24,8),
BS_21094 decimal(24,8),
BS_21097 decimal(24,8),
BS_21100 decimal(24,8),
BS_22001 decimal(24,8),
BS_22004 decimal(24,8),
BS_22007 decimal(24,8),
BS_22010 decimal(24,8),
BS_22013 decimal(24,8),
BS_22019 decimal(24,8),
BS_22022 decimal(24,8),
BS_30005 decimal(24,8),
BS_30006 decimal(24,8),
BS_40000 decimal(24,8),
BS_1100101 decimal(24,8),
BS_11004 decimal(24,8),
BS_1100401 decimal(24,8),
BS_11007 decimal(24,8),
BS_11010 decimal(24,8),
BS_11013 decimal(24,8),
BS_11019 decimal(24,8),
BS_11022 decimal(24,8),
BS_11025 decimal(24,8),
BS_11028 decimal(24,8),
BS_11034 decimal(24,8),
BS_11037 decimal(24,8),
BS_11043 decimal(24,8),
BS_11046 decimal(24,8),
BS_11049 decimal(24,8),
BS_11052 decimal(24,8),
BS_11055 decimal(24,8),
BS_11058 decimal(24,8),
BS_11061 decimal(24,8),
BS_11064 decimal(24,8),
BS_1107301 decimal(24,8),
BS_11076 decimal(24,8),
BS_CASPEC decimal(24,8),
BS_12002 decimal(24,8),
BS_12004 decimal(24,8),
BS_12007 decimal(24,8),
BS_12010 decimal(24,8),
BS_1204601 decimal(24,8),
BS_12061 decimal(24,8),
BS_12067 decimal(24,8),
BS_NCASPEC decimal(24,8),
BS_ASPEC decimal(24,8),
BS_2100101 decimal(24,8),
BS_21004 decimal(24,8),
BS_21007 decimal(24,8),
BS_21010 decimal(24,8),
BS_21013 decimal(24,8),
BS_21016 decimal(24,8),
BS_21022 decimal(24,8),
BS_21025 decimal(24,8),
BS_21028 decimal(24,8),
BS_21031 decimal(24,8),
BS_21034 decimal(24,8),
BS_21037 decimal(24,8),
BS_21040 decimal(24,8),
BS_21043 decimal(24,8),
BS_21046 decimal(24,8),
BS_21049 decimal(24,8),
BS_21052 decimal(24,8),
BS_21055 decimal(24,8),
BS_21058 decimal(24,8),
BS_21061 decimal(24,8),
BS_21064 decimal(24,8),
BS_21067 decimal(24,8),
BS_CLSPEC decimal(24,8),
BS_22016 decimal(24,8),
BS_22025 decimal(24,8),
BS_NCLSPEC decimal(24,8),
BS_LSPEC decimal(24,8),
BS_31007 decimal(24,8),
BS_31022 decimal(24,8),
BS_31025 decimal(24,8),
BS_PCESPEC decimal(24,8),
BS_ESPEC decimal(24,8),
BS_LESPEC decimal(24,8),
SPEC_DES string,
INFO_PUB_DATE string,
BS_31037 decimal(24,8),
BS_11029 decimal(24,8),
BS_21068 decimal(24,8),
BS_11017 decimal(24,8),
BS_11026 decimal(24,8),
BS_11027 decimal(24,8),
BS_12003 decimal(24,8),
BS_12005 decimal(24,8),
BS_12011 decimal(24,8),
BS_12012 decimal(24,8),
BS_12032 decimal(24,8),
BS_21017 decimal(24,8),
BS_21032 decimal(24,8),
BS_22005 decimal(24,8),
BS_22017 decimal(24,8),
BS_11018 decimal(24,8),
BS_21026 decimal(24,8),
UNIQUE_INDEX string,
IS_DEL int,
sink_table string,
proctime as PROCTIME(),
eventTime AS cast(UPDATETIME as TIMESTAMP(0)),
WATERMARK FOR UPDATETIME AS UPDATETIME - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'dwd_fin_bala_short',
  'properties.bootstrap.servers' = 'master:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
)

-- sink
CREATE TABLE stkinfo (
  isvalid string ,
  createtime varchar(30) ,
  updatetime varchar(30) ,
  stk_uni_code varchar(12),
  com_uni_code bigint ,
  stk_code string ,
  stk_short_name varchar(50) ,
  spe_short_name varchar(50) ,
  stk_type_par string ,
  list_date string ,
  sec_mar_par string ,
  list_sect_par string ,
  list_sta_par string ,
  iss_sta_par string ,
  isin_code string ,
  end_date string ,
  belong_park string ,
  trans_way string ,
  delist_type_par string ,
  list_system string ,
  stk_cdr_ratio string ,
  unique_index string ,
  is_del string ,
  wm as cast(updatetime as timestamp_ltz(3)),
  WATERMARK FOR wm as wm,
  PRIMARY KEY (stk_uni_code) NOT ENFORCED
) WITH (
    'connector.type' = 'jdbc',
    'connector.url' = 'jdbc:mysql://bj-cynosdbmysql-grp-rzunn0bc.sql.tencentcdb.com:20452/dataplat_ads',
    'connector.table' = 'dim_stk_basic_info',
    'connector.username' = 'star_test',
    'connector.password' = '3iu8dn4H#2JD',
    'connector.write.flush.max-rows' = '1'
);


SELECT
o.COM_UNI_CODE,
stkinfo.stk_uni_code,
stkinfo.stk_code
FROM finbalashort as o
LEFT JOIN stkinfo FOR SYSTEM_TIME AS OF o.proctime
ON o.COM_UNI_CODE = stkinfo.com_uni_code