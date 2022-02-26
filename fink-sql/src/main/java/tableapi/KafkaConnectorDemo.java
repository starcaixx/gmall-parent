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
        tableEnv.executeSql(
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
        table.execute().print();
    }
}
