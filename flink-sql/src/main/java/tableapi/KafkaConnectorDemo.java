package tableapi;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class KafkaConnectorDemo {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

//        List<String> sql = Files.readAllLines(path);
//        tableEnv.executeSql(sql.get(0));
//        tableEnv.executeSql("SET 'table.local-time-zone' = 'UTC'");
        /*tableEnv.executeSql(
                "CREATE TABLE user_log1 (\n" +
                        "  user_id string,\n" +
                        "  item_id string,\n" +
                        "  category_id string,\n" +
                        "  behavior STRING,\n" +
                        "  ts TIMESTAMP_LTZ(0),\n" +
                        "  WATERMARK FOR ts AS ts - INTERVAL '2' SECOND\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'user_behavior',\n" +
                        " 'properties.group.id' = 'testGroup'," +
                        "  'properties.bootstrap.servers' = 'master:9092',\n" +
                        "  'scan.startup.mode' = 'latest-offset',"  +
                        "  'format' = 'json'\n" +
                        ")");*/
        tableEnv.executeSql("CREATE TABLE KafkaTable (\n" +
                "  `user_id` BIGINT,\n" +
                "  `item_id` BIGINT,\n" +
                "  `category_id` STRING,\n" +
                "  `behavior` STRING,\n" +
                " ts STRING,\n"+
                " wm as cast(concat(substr(ts,1,10),' ',substr(ts,12,8)) as timestamp_ltz(0)),\n"+
                "  WATERMARK FOR wm as wm - INTERVAL '10' SECOND\n" +
//                "  `ts` TIMESTAMP(3) METADATA FROM 'timestamp'\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'user_behavior',\n" +
                "  'properties.bootstrap.servers' = 'master:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");


        tableEnv.executeSql(
                "CREATE TABLE pvuv_sink ( \n" +
                        "  dt STRING, \n" +
                        "  pv BIGINT, \n" +
                        "  uv BIGINT, \n" +
                        "  PRIMARY KEY (dt) NOT ENFORCED \n" +
                        ") WITH ( \n" +
                        "   'connector' = 'jdbc', \n" +
                        "   'url' = 'jdbc:mysql://bj-cynosdbmysql-grp-rzunn0bc.sql.tencentcdb.com:20452/flink-test', \n" +
                        "   'table-name' = 'pvuv_sink',\n" +
                        "   'username' = 'star_test',\n" +
                        "   'password' = '3iu8dn4H#2JD'\n" +
                        ")"
        );

//        Table table = tableEnv.from("KafkaTable");

        tableEnv.executeSql(
                "INSERT INTO pvuv_sink\n" +
                "SELECT\n" +
                "  DATE_FORMAT(wm, 'yyyy-MM-dd HH:00') dt,\n" +
                "  COUNT(*) AS pv,\n" +
                "  COUNT(DISTINCT user_id) AS uv\n" +
                "FROM KafkaTable\n" +
                "GROUP BY DATE_FORMAT(wm, 'yyyy-MM-dd HH:00')").print();
//        table.execute().print();
    }
}
