package tableapi;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import java.nio.file.Files;
import java.util.List;

public class KafkaConnectorDemo {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

//        List<String> sql = Files.readAllLines(path);
//        tableEnv.executeSql(sql.get(0));
        /*tableEnv.executeSql(
                "CREATE TABLE user_log1 (\n" +
                        "  user_id string,\n" +
                        "  item_id string,\n" +
                        "  category_id string,\n" +
                        "  behavior STRING,\n" +
                        "  ts TIMESTAMP(1) WITH LOCAL TIME ZONE,\n" +
                        "  WATERMARK FOR ts AS ts - INTERVAL '2' SECOND\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'user_behavior',\n" +
                        " 'properties.group.id' = 'testGroup'," +
                        "  'properties.bootstrap.servers' = 'node4:9092',\n" +
                        "  'scan.startup.mode' = 'latest-offset',"  +
                        "  'format' = 'json'\n" +
                        ")");

        Table table = tableEnv.from("user_log1");
        table.execute().print();*/
        tableEnv.executeSql(
                "CREATE TABLE finbalashort (\n" +
                        "ISVALID int,\n" +
                        "CREATETIME TIMESTAMP(6),\n" +
                        "UPDATETIME TIMESTAMP(3),\n" +
                        "COM_UNI_CODE bigint,\n" +
                        "END_DATE TIMESTAMP(6),\n" +
                        "CURY_TYPE_PAR int,\n" +
                        "CURY_UNIT_PAR int,\n" +
                        "BS_11001 decimal(24,8),\n" +
                        "BS_11002 decimal(24,8),\n" +
                        "BS_11003 decimal(24,8),\n" +
                        "BS_11000 decimal(24,8),\n" +
                        "BS_12001 decimal(24,8),\n" +
                        "BS_10000 decimal(24,8),\n" +
                        "BS_21001 decimal(24,8),\n" +
                        "BS_21002 decimal(24,8),\n" +
                        "BS_21000 decimal(24,8),\n" +
                        "BS_22000 decimal(24,8),\n" +
                        "BS_20000 decimal(24,8),\n" +
                        "BS_30001 decimal(24,8),\n" +
                        "BS_30002 decimal(24,8),\n" +
                        "BS_30003 decimal(24,8),\n" +
                        "BS_30004 decimal(24,8),\n" +
                        "BS_31000 decimal(24,8),\n" +
                        "BS_32000 decimal(24,8),\n" +
                        "BS_30000 decimal(24,8),\n" +
                        "BS_11016 decimal(24,8),\n" +
                        "BS_11031 decimal(24,8),\n" +
                        "BS_11067 decimal(24,8),\n" +
                        "BS_11070 decimal(24,8),\n" +
                        "BS_11079 decimal(24,8),\n" +
                        "BS_11082 decimal(24,8),\n" +
                        "BS_12013 decimal(24,8),\n" +
                        "BS_12016 decimal(24,8),\n" +
                        "BS_12019 decimal(24,8),\n" +
                        "BS_12022 decimal(24,8),\n" +
                        "BS_12025 decimal(24,8),\n" +
                        "BS_12031 decimal(24,8),\n" +
                        "BS_12034 decimal(24,8),\n" +
                        "BS_12037 decimal(24,8),\n" +
                        "BS_12040 decimal(24,8),\n" +
                        "BS_12043 decimal(24,8),\n" +
                        "BS_12046 decimal(24,8),\n" +
                        "BS_12049 decimal(24,8),\n" +
                        "BS_12052 decimal(24,8),\n" +
                        "BS_12055 decimal(24,8),\n" +
                        "BS_12058 decimal(24,8),\n" +
                        "BS_12064 decimal(24,8),\n" +
                        "BS_12000 decimal(24,8),\n" +
                        "BS_21003 decimal(24,8),\n" +
                        "BS_21019 decimal(24,8),\n" +
                        "BS_21070 decimal(24,8),\n" +
                        "BS_21079 decimal(24,8),\n" +
                        "BS_21082 decimal(24,8),\n" +
                        "BS_21085 decimal(24,8),\n" +
                        "BS_21088 decimal(24,8),\n" +
                        "BS_21091 decimal(24,8),\n" +
                        "BS_21094 decimal(24,8),\n" +
                        "BS_21097 decimal(24,8),\n" +
                        "BS_21100 decimal(24,8),\n" +
                        "BS_22001 decimal(24,8),\n" +
                        "BS_22004 decimal(24,8),\n" +
                        "BS_22007 decimal(24,8),\n" +
                        "BS_22010 decimal(24,8),\n" +
                        "BS_22013 decimal(24,8),\n" +
                        "BS_22019 decimal(24,8),\n" +
                        "BS_22022 decimal(24,8),\n" +
                        "BS_30005 decimal(24,8),\n" +
                        "BS_30006 decimal(24,8),\n" +
                        "BS_40000 decimal(24,8),\n" +
                        "BS_1100101 decimal(24,8),\n" +
                        "BS_11004 decimal(24,8),\n" +
                        "BS_1100401 decimal(24,8),\n" +
                        "BS_11007 decimal(24,8),\n" +
                        "BS_11010 decimal(24,8),\n" +
                        "BS_11013 decimal(24,8),\n" +
                        "BS_11019 decimal(24,8),\n" +
                        "BS_11022 decimal(24,8),\n" +
                        "BS_11025 decimal(24,8),\n" +
                        "BS_11028 decimal(24,8),\n" +
                        "BS_11034 decimal(24,8),\n" +
                        "BS_11037 decimal(24,8),\n" +
                        "BS_11043 decimal(24,8),\n" +
                        "BS_11046 decimal(24,8),\n" +
                        "BS_11049 decimal(24,8),\n" +
                        "BS_11052 decimal(24,8),\n" +
                        "BS_11055 decimal(24,8),\n" +
                        "BS_11058 decimal(24,8),\n" +
                        "BS_11061 decimal(24,8),\n" +
                        "BS_11064 decimal(24,8),\n" +
                        "BS_1107301 decimal(24,8),\n" +
                        "BS_11076 decimal(24,8),\n" +
                        "BS_CASPEC decimal(24,8),\n" +
                        "BS_12002 decimal(24,8),\n" +
                        "BS_12004 decimal(24,8),\n" +
                        "BS_12007 decimal(24,8),\n" +
                        "BS_12010 decimal(24,8),\n" +
                        "BS_1204601 decimal(24,8),\n" +
                        "BS_12061 decimal(24,8),\n" +
                        "BS_12067 decimal(24,8),\n" +
                        "BS_NCASPEC decimal(24,8),\n" +
                        "BS_ASPEC decimal(24,8),\n" +
                        "BS_2100101 decimal(24,8),\n" +
                        "BS_21004 decimal(24,8),\n" +
                        "BS_21007 decimal(24,8),\n" +
                        "BS_21010 decimal(24,8),\n" +
                        "BS_21013 decimal(24,8),\n" +
                        "BS_21016 decimal(24,8),\n" +
                        "BS_21022 decimal(24,8),\n" +
                        "BS_21025 decimal(24,8),\n" +
                        "BS_21028 decimal(24,8),\n" +
                        "BS_21031 decimal(24,8),\n" +
                        "BS_21034 decimal(24,8),\n" +
                        "BS_21037 decimal(24,8),\n" +
                        "BS_21040 decimal(24,8),\n" +
                        "BS_21043 decimal(24,8),\n" +
                        "BS_21046 decimal(24,8),\n" +
                        "BS_21049 decimal(24,8),\n" +
                        "BS_21052 decimal(24,8),\n" +
                        "BS_21055 decimal(24,8),\n" +
                        "BS_21058 decimal(24,8),\n" +
                        "BS_21061 decimal(24,8),\n" +
                        "BS_21064 decimal(24,8),\n" +
                        "BS_21067 decimal(24,8),\n" +
                        "BS_CLSPEC decimal(24,8),\n" +
                        "BS_22016 decimal(24,8),\n" +
                        "BS_22025 decimal(24,8),\n" +
                        "BS_NCLSPEC decimal(24,8),\n" +
                        "BS_LSPEC decimal(24,8),\n" +
                        "BS_31007 decimal(24,8),\n" +
                        "BS_31022 decimal(24,8),\n" +
                        "BS_31025 decimal(24,8),\n" +
                        "BS_PCESPEC decimal(24,8),\n" +
                        "BS_ESPEC decimal(24,8),\n" +
                        "BS_LESPEC decimal(24,8),\n" +
                        "SPEC_DES string,\n" +
                        "INFO_PUB_DATE string,\n" +
                        "BS_31037 decimal(24,8),\n" +
                        "BS_11029 decimal(24,8),\n" +
                        "BS_21068 decimal(24,8),\n" +
                        "BS_11017 decimal(24,8),\n" +
                        "BS_11026 decimal(24,8),\n" +
                        "BS_11027 decimal(24,8),\n" +
                        "BS_12003 decimal(24,8),\n" +
                        "BS_12005 decimal(24,8),\n" +
                        "BS_12011 decimal(24,8),\n" +
                        "BS_12012 decimal(24,8),\n" +
                        "BS_12032 decimal(24,8),\n" +
                        "BS_21017 decimal(24,8),\n" +
                        "BS_21032 decimal(24,8),\n" +
                        "BS_22005 decimal(24,8),\n" +
                        "BS_22017 decimal(24,8),\n" +
                        "BS_11018 decimal(24,8),\n" +
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
        );

//        tableEnv.from("KafkaTable").execute().print();

        tableEnv.executeSql(
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
                        "    'connector.url' = 'jdbc:mysql://master:20452/dataplat_ads',\n" +
                        "    'connector.table' = 'dim_stk_basic_info',\n" +
                        "    'connector.username' = 'xxxxx',\n" +
                        "    'connector.password' = '123456',\n" +
                        "    'connector.write.flush.max-rows' = '1'\n" +
                        ")"
        );

//        tableEnv.from("stkinfo").execute().print();
        tableEnv.executeSql(
                "SELECT \n" +
                        "o.COM_UNI_CODE,\n" +
                        "stkinfo.stk_uni_code,\n" +
                        "stkinfo.stk_code\n" +
                        "FROM finbalashort as o \n" +
                        "LEFT JOIN stkinfo FOR SYSTEM_TIME AS OF o.proctime  \n" +
                        "ON o.COM_UNI_CODE = stkinfo.com_uni_code"
        ).print();
    }
}
