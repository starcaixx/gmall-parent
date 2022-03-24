package tableapi;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import java.nio.file.Files;
import java.util.List;

public class KafkaConnectorDemo {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
//                .inBatchMode()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

//        List<String> sql = Files.readAllLines(path);
//        tableEnv.executeSql(sql.get(0));

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.executeSql("drop table if exists kafka_dwd_upchina_res_sec_rate");
        tableEnv.executeSql("CREATE TABLE kafka_dwd_upchina_res_sec_rate (\n" +
                "`partition` BIGINT METADATA VIRTUAL," +
                "`offset` BIGINT METADATA VIRTUAL," +
                "  `ISVALID` SMALLINT,\n" +
                "  `RES_ID` bigint,\n" +
                "  `SEC_UNI_CODE` bigint,\n" +
                "  `RES_RATE_PAR` SMALLINT,\n" +
                "  `LAST_RATE_PAR` SMALLINT,\n" +
                "  `IS_FIRST` SMALLINT,\n" +
                "  `RATE_CHG_PAR` SMALLINT,\n" +
                "  `CUR_TYPE_PAR` SMALLINT,\n" +
                "  `LOW_EXPE_PRICE` decimal(20,2),\n" +
                "  `HIGH_EXPE_PRICE` decimal(20,2),\n" +
                "  `EXPE_PERIOD_PAR` SMALLINT,\n" +
                "  `EXPE_START_DATE` string,\n" +
                "  `EXPE_END_DATE` string,\n" +
                "  `CREATETIME` TIMESTAMP(3),\n" +
                "  `UPDATETIME` TIMESTAMP(3),\n" +
                "  `PRICE_CHAN_PAR` SMALLINT,\n" +
                "  WATERMARK FOR UPDATETIME as UPDATETIME - INTERVAL '10' SECOND \n"
                + " ) WITH ("
                + " 'connector' = 'kafka',"
                + " 'topic' = 'dwd_upchina_res_sec_rate',"
                + " 'properties.bootstrap.servers' = 'master:9092',"
                + " 'properties.group.id' = 'flinkkafkademo',"
                + " 'scan.startup.mode' = 'specific-offsets',"
                + " 'scan.startup.specific-offsets' = '" + "partition:0,offset:0" + "',"
                + " 'format' = 'json'"
                + ")");

        tableEnv.from("kafka_dwd_upchina_res_sec_rate").limit(10).execute().print();

        /*tableEnv.executeSql(
                "SELECT " +
                "`partition`," +
                "`offset`," +
                "ISVALID,RES_ID,SEC_UNI_CODE,RES_RATE_PAR,LAST_RATE_PAR," +
                "IS_FIRST,RATE_CHG_PAR,CUR_TYPE_PAR,LOW_EXPE_PRICE," +
                "HIGH_EXPE_PRICE,EXPE_PERIOD_PAR,EXPE_START_DATE,EXPE_END_DATE," +
                "CREATETIME,UPDATETIME,PRICE_CHAN_PAR," +
                "DATE_FORMAT(UPDATETIME, 'yyyy-MM-dd'), " +
                "DATE_FORMAT(UPDATETIME, 'HH') " +
                "FROM kafka_dwd_upchina_res_sec_rate limit 10");*/

        /*tableEnv.executeSql(
                "CREATE TABLE user_log1 (\n" +
                        "`partition` BIGINT METADATA VIRTUAL," +
                        "`offset` BIGINT METADATA VIRTUAL," +
                        "  user_id string,\n" +
                        "  item_id string,\n" +
                        "  category_id string,\n" +
                        "  behavior STRING,\n" +
                        "  ts STRING,\n" +
                        " wm as cast(concat(substr(ts,1,10),' ',substr(ts,12,8)) as timestamp_ltz(0)), " +
                        "  WATERMARK FOR wm as wm - INTERVAL '10' SECOND \n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'user_behavior',\n" +
                        " 'properties.group.id' = 'testGroup'," +
                        "  'properties.bootstrap.servers' = 'master:9092',\n" +
                        "  'scan.startup.mode' = 'earliest-offset',"  +
                        "  'format' = 'json'\n" +
                        ")");

//        tableEnv.from("user_log1").execute().print();
        tableEnv.executeSql(
                "SELECT " +
                "`partition`," +
                "`offset`," +
                "user_id," +
                "item_id," +
                "category_id," +
                "behavior," +
                "DATE_FORMAT(wm, 'yyyy-MM-dd'), " +
                "DATE_FORMAT(wm, 'HH') " +
                "FROM user_log1 ").print();*/

        /*tableEnv.executeSql(
                "CREATE TABLE finbalashort (\n" +
                        "ISVALID int,\n" +
                        "CREATETIME TIMESTAMP(6),\n" +
                        "UPDATETIME TIMESTAMP(3),\n" +
                        "BS_21026 decimal(24,8),\n" +
                        "UNIQUE_INDEX string,\n" +
                        "IS_DEL int,\n" +
                        "sink_table string,\n" +
                        "proctime as PROCTIME(),\n" +
                        "eventTime AS cast(UPDATETIME as TIMESTAMP(0)),\n" +
                        "WATERMARK FOR UPDATETIME AS UPDATETIME - INTERVAL '5' SECOND\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'dwd_fin_bala_short',\n" +
                        "  'properties.bootstrap.servers' = 'master:9092',\n" +
                        "  'properties.group.id' = 'testGroup',\n" +
                        "  'scan.startup.mode' = 'earliest-offset',\n" +
                        "  'format' = 'json'\n" +
                        ")"
        );*/

//        tableEnv.from("KafkaTable").execute().print();

        /*tableEnv.executeSql(
                "CREATE TABLE stkinfo (\n" +
                        "  isvalid string ,\n" +
                        "  createtime varchar(30) ,\n" +
                        "  updatetime varchar(30) ,\n" +
                        "  stk_uni_code varchar(12),\n" +
                        "  com_uni_code bigint ,\n" +
                        "  stk_code string ,\n" +
                        "  stk_short_name varchar(50) ,\n" +
                        "  spe_short_name varchar(50) ,\n" +
                        "  stk_type_par string ,\n" +
                        "  list_date string ,\n" +
                        "  sec_mar_par string ,\n" +
                        "  list_sect_par string ,\n" +
                        "  list_sta_par string ,\n" +
                        "  iss_sta_par string ,\n" +
                        "  isin_code string ,\n" +
                        "  end_date string ,\n" +
                        "  belong_park string ,\n" +
                        "  trans_way string ,\n" +
                        "  delist_type_par string ,\n" +
                        "  list_system string ,\n" +
                        "  stk_cdr_ratio string ,\n" +
                        "  unique_index string ,\n" +
                        "  is_del string ,\n" +
                        "  PRIMARY KEY (stk_uni_code) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        "    'connector.type' = 'jdbc',\n" +
                        "    'connector.url' = 'jdbc:mysql://bj-cynosdbmysql-grp-rzunn0bc.sql.tencentcdb.com:20452/dataplat_ads',\n" +
                        "    'connector.table' = 'dim_stk_basic_info',\n" +
                        "    'connector.username' = 'star_test',\n" +
                        "    'connector.password' = '3iu8dn4H#2JD',\n" +
                        "    'connector.write.flush.max-rows' = '1'\n" +
                        ")"
        );

        tableEnv.from("stkinfo").execute().print();*/
        /*tableEnv.executeSql(
                "SELECT \n" +
                        "o.COM_UNI_CODE,\n" +
                        "stkinfo.stk_uni_code,\n" +
                        "stkinfo.stk_code\n" +
                        "FROM finbalashort as o \n" +
                        "LEFT JOIN stkinfo FOR SYSTEM_TIME AS OF o.proctime  \n" +
                        "ON o.COM_UNI_CODE = stkinfo.com_uni_code"
        ).print();*/
    }
}
