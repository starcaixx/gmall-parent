SET pipeline.name=test-aaa;
CREATE TABLE finbalashort (
ISVALID int,
CREATETIME TIMESTAMP(6),
UPDATETIME TIMESTAMP(3),
COM_UNI_CODE bigint,
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
);

CREATE TABLE stkinfo (
  createtime varchar(30) ,
  updatetime varchar(30) ,
  stk_uni_code varchar(12),
  com_uni_code bigint ,
  stk_code string ,
  stk_short_name varchar(50) ,
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
ON o.COM_UNI_CODE = stkinfo.com_uni_code;