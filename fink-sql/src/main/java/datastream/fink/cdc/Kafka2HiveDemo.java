package datastream.fink.cdc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.hadoop.hive.conf.HiveConf;

import java.time.Duration;

public class Kafka2HiveDemo {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("pipeline.name", "kafka2hive");
        configuration.setString("table.exec.hive.fallback-mapred-reader","true");
        configuration.setString("table.local-time-zone", "UTC");
        configuration.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        configuration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMillis(1000*60*10));

        configuration.setString("heartbeat.timeout", "180000");

        System.setProperty("HADOOP_USER_NAME","star");
        String name            = "myhive";
        String defaultDatabase = "ods";
        String hiveConfDir     = "/opt/module/hive-conf";
//        String hiveConfDir     = "E:\\hive-conf";
//        String hiveConfDir     = "/Users/star/develop/module/hive-conf";
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        HiveConf hiveConf = hive.getHiveConf();
        hiveConf.set("dfs.datanode.use.datanode.hostname","true");
        hiveConf.set("dfs.client.use.datanode.hostname","true");
        hiveConf.set("dfs.replication", "1");
        tableEnv.registerCatalog(name, hive);

// set the HiveCatalog as the current catalog of the session
        tableEnv.useCatalog(name);
//hive语义
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        tableEnv.executeSql("drop table if exists app_log");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS app_log (" +
                "partition_id bigint ," +
                "offset_id bigint ," +
                "  user_id string,\n" +
                "  item_id string,\n" +
                "  category_id string,\n" +
                "  behavior STRING \n"
                + " ) PARTITIONED BY (dt STRING, hr STRING) STORED AS orc TBLPROPERTIES ("
                + " 'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00',"
                + " 'sink.partition-commit.trigger'='partition-time',"
                + " 'sink.partition-commit.delay'='60s',"
                + " 'sink.partition-commit.policy.kind'='metastore,success-file',"
                + " 'sink.partition-commit.success-file.name' = '_SUCCESS',"
                + " 'auto-compaction' = 'true'"
                + ")");

        //flinksql语义
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.executeSql("drop table if exists kafka_data_app_log");
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS kafka_data_app_log (" +
                "`partition` BIGINT METADATA VIRTUAL," +
                "`offset` BIGINT METADATA VIRTUAL," +
                "  user_id string,\n" +
                "  item_id string,\n" +
                "  category_id string,\n" +
                "  behavior STRING,\n" +
                "  ts STRING,\n" +
                " wm as cast(concat(substr(ts,1,10),' ',substr(ts,12,8)) as timestamp_ltz(0)), " +
                "  WATERMARK FOR wm as wm - INTERVAL '10' SECOND \n"
                + " ) WITH ("
                + " 'connector' = 'kafka',"
                + " 'topic' = 'user_behavior',"
                + " 'properties.bootstrap.servers' = 'master:9092',"
                + " 'properties.group.id' = 'flinkkafkademo',"
                + " 'scan.startup.mode' = 'specific-offsets',"
                + " 'scan.startup.specific-offsets' = '" + "partition:0,offset:0" + "',"
                + " 'format' = 'json'"
                + ")");

        tableEnv.executeSql("INSERT INTO app_log " +
                "SELECT " +
                "`partition`," +
                "`offset`," +
                "user_id," +
                "item_id," +
                "category_id," +
                "behavior," +
                "DATE_FORMAT(wm, 'yyyy-MM-dd'), " +
                "DATE_FORMAT(wm, 'HH') " +
                "FROM kafka_data_app_log ");

//        tableEnv.executeSql("select name,count(1) from student group by name").print();
//        tableEnv.executeSql("select * from student limit 10").print();


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
