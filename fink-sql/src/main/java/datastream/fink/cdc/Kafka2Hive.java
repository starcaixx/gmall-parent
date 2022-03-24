package datastream.fink.cdc;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.hadoop.hive.conf.HiveConf;

public class Kafka2Hive {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String name            = "myhive";
        String defaultDatabase = "ods";
//        String hiveConfDir     = "/opt/module/hive-conf";
        String hiveConfDir     = "/Users/star/develop/module/hive-conf";
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        HiveConf hiveConf = hive.getHiveConf();
        hiveConf.set("dfs.datanode.use.datanode.hostname","true");
        hiveConf.set("dfs.client.use.datanode.hostname","true");
        hiveConf.set("dfs.replication", "1");
        tableEnv.registerCatalog("myhive", hive);

// set the HiveCatalog as the current catalog of the session
        tableEnv.useCatalog("myhive");
        tableEnv.executeSql("select * from student limit 10").print();


        /*tableEnv.executeSql("CREATE TABLE dwd_upchina_res_sec_rate (\n" +
                "   `ISVALID` SMALLINT,\n" +
                "   `RES_ID` bigint,\n" +
                "   `SEC_UNI_CODE` bigint,\n" +
                "   `RES_RATE_PAR` SMALLINT,\n" +
                "   `LAST_RATE_PAR` SMALLINT,\n" +
                "   `IS_FIRST` SMALLINT,\n" +
                "   `RATE_CHG_PAR` SMALLINT,\n" +
                "   `CUR_TYPE_PAR` SMALLINT,\n" +
                "   `LOW_EXPE_PRICE` decimal(20,2),\n" +
                "   `HIGH_EXPE_PRICE` decimal(20,2),\n" +
                "   `EXPE_PERIOD_PAR` SMALLINT,\n" +
                "   `EXPE_START_DATE` string,\n" +
                "   `EXPE_END_DATE` string,\n" +
                "   `CREATETIME` TIMESTAMP(6),\n" +
                "   `UPDATETIME` TIMESTAMP(6),\n" +
                "   `PRICE_CHAN_PAR` SMALLINT\n" +
                " ) WITH (\n" +
                "   'connector' = 'kafka',\n" +
                "   'topic' = 'dwd_upchina_res_sec_rate',\n" +
                "   'properties.bootstrap.servers' = 'master:9092',\n" +
                "   'properties.group.id' = 'testGroup',\n" +
                "   'scan.startup.mode' = 'earliest-offset',\n" +
                "   'format' = 'json'\n" +
                " )");*/

//        tableEnv.from("dwd_upchina_res_sec_rate").execute().print();
//        tableEnv.executeSql("select * from dwd_upchina_res_sec_rate limit 10").print();
    }
}
