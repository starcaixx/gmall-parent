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

public class Kafka2Hive {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("pipeline.name", "kafka2hive-dwd_upchina_res_sec_rate");
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

        tableEnv.executeSql("DROP TABLE IF EXISTS dwd_upchina_res_sec_rate");
        tableEnv.executeSql("" +
                "CREATE TABLE IF NOT EXISTS  dwd_upchina_res_sec_rate (\n" +
                "partition_id bigint ," +
                "offset_id bigint ," +
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
                "  `CREATETIME` TIMESTAMP,\n" +
                "  `UPDATETIME` TIMESTAMP,\n" +
                "  `PRICE_CHAN_PAR` SMALLINT\n"
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
                + " 'properties.group.id' = 'flinkkafkademo'," +
                "  'scan.startup.mode' = 'earliest-offset',\n"
//                + " 'scan.startup.mode' = 'specific-offsets',"
//                + " 'scan.startup.specific-offsets' = '" + "partition:0,offset:0" + "',"
                + " 'format' = 'json'"
                + ")");

        tableEnv.executeSql("INSERT INTO dwd_upchina_res_sec_rate " +
                "SELECT " +
                        "`partition`," +
                        "`offset`," +
                        "ISVALID,RES_ID,SEC_UNI_CODE,RES_RATE_PAR,LAST_RATE_PAR," +
                        "IS_FIRST,RATE_CHG_PAR,CUR_TYPE_PAR,LOW_EXPE_PRICE," +
                        "HIGH_EXPE_PRICE,EXPE_PERIOD_PAR,EXPE_START_DATE,EXPE_END_DATE," +
                        "CREATETIME,UPDATETIME,PRICE_CHAN_PAR," +
                        "DATE_FORMAT(UPDATETIME, 'yyyy-MM-dd'), " +
                        "DATE_FORMAT(UPDATETIME, 'HH') " +
                        "FROM kafka_dwd_upchina_res_sec_rate");
    }
}
