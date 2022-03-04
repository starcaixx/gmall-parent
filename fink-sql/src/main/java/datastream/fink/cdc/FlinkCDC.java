package datastream.fink.cdc;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.module.paranamer.ParanamerModule;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.time.Duration;

public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

//        env.setRestartStrategy(RestartStrategies.noRestart());

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new ParanamerModule());

        objectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.configure(JsonGenerator.Feature.IGNORE_UNKNOWN, true);
        objectMapper.configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
        objectMapper.configure(JsonParser.Feature.ALLOW_MISSING_VALUES, true);
//        objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES,false);
        objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        DebeziumSourceFunction<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("rm-bp146k5r8kw09t21xho.mysql.rds.aliyuncs.com")
                .port(3306)
                .username("star_test")
                .password("STARcai01230")
                .databaseList("gmall")
                .tableList("gmall.user_info")
                .startupOptions(StartupOptions.initial())
                .deserializer(new DebeziumDeserializationSchema(){

                    @Override
                    public TypeInformation getProducedType() {
                        return TypeInformation.of(String.class);
                    }

                    @Override
                    public void deserialize(SourceRecord sourceRecord, Collector collector) throws Exception {
                        String topic = sourceRecord.topic();
                        String[] arr = topic.split("\\.");
                        String db = arr[1];
                        String tableName = arr[2];

                        //获取操作类型 READ DELETE UPDATE CREATE
                        Envelope.Operation operation = Envelope.operationFor(sourceRecord);


                        //获取值信息并转换为Struct类型
                        Struct value = (Struct) sourceRecord.value();

                        String ts_ms = value.get("ts_ms").toString();
                        //获取变化后的数据
                        Struct after = value.getStruct("after");

                        ObjectNode data = objectMapper.createObjectNode();

                        for (Field field : after.schema().fields()) {
                            Object o = after.get(field);
                            data.put(field.name(), o==null?null:o.toString());
                        }
                        //创建JSON对象用于存储数据信息
                        ObjectNode objectNode = objectMapper.createObjectNode();

                        //创建JSON对象用于封装最终返回值数据信息
                        objectNode.put("operation", operation.toString().toLowerCase());
                        objectNode.put("data", data);
                        objectNode.put("database", db);
                        objectNode.put("table", tableName);
                        objectNode.put("timestamp", ts_ms);

                        String result = objectMapper.writeValueAsString(objectNode);

                        System.out.println("result:"+result);
                        //发送数据至下游
                        collector.collect(result);
                    }
                })
                .build();

        DataStreamSource<String> mysqlDS = env.addSource(mysqlSource);

        mysqlDS.print(">>>>>>>>>>>>>>>>>>>>");

        String[] fieldNames  =
                {"id","login_name","nick_name","passwd","name","phone_num","email","head_img","user_level","birthday","gender","create_time","operate_time","status","operation"};

        TypeInformation[] types =
                {Types.INT,Types.STRING,Types.STRING,Types.STRING,Types.STRING,Types.STRING,Types.STRING,
                        Types.STRING,Types.STRING,Types.LOCAL_DATE,Types.STRING,Types.LOCAL_DATE_TIME,
                        Types.LOCAL_DATE_TIME,Types.STRING,Types.STRING};

        SingleOutputStreamOperator<Row> rowDS = mysqlDS.map(record -> {
            JsonNode jsonNode = objectMapper.readValue(record, JsonNode.class);
            int arity = fieldNames.length;
            JsonNode data = jsonNode.get("data");
            Row row = new Row(arity);
            row.setField(0, data.get("id").asText());
            row.setField(1, data.get("login_name").asText());
            row.setField(2, data.get("nick_name").asText());
            row.setField(3, data.get("passwd").asText());
            row.setField(4, data.get("name").asText());
            row.setField(5, data.get("phone_num").asText());
            row.setField(6, data.get("email").asText());
            row.setField(7, data.get("head_img").asText());
            row.setField(8, data.get("user_level").asText());
            row.setField(9, data.get("birthday").asText());
            row.setField(10, data.get("gender").asText());
            row.setField(11, data.get("create_time").asText());
            row.setField(12, data.get("operate_time").asText());
            row.setField(13, data.get("status").asText());
            row.setField(14, jsonNode.get("timestamp").asText());
            row.setField(15, jsonNode.get("operation").asText());
            return row;
        });

        SingleOutputStreamOperator<Row> withWmDS = rowDS.assignTimestampsAndWatermarks(WatermarkStrategy.
                <Row>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((row, timestamp) -> Long.valueOf(row.getField(14).toString())));

        EnvironmentSettings tableEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, tableEnvSettings);

        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE,CheckpointingMode.EXACTLY_ONCE);
        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMillis(1));
        String catalogName = "devHive";
        HiveCatalog hiveCatalog = new HiveCatalog(catalogName, "ecommerce", "/Users/star/develop/module/apache-hive-3.1.2-bin/conf");
        tableEnv.registerCatalog(catalogName,hiveCatalog);
        tableEnv.useCatalog(catalogName);

        tableEnv.createTemporaryView("merged_order_info", withWmDS);

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS ods");
        tableEnv.executeSql("DROP TABLE IF EXISTS ods.order_info");
        tableEnv.executeSql("CREATE TABLE ods.order_info (\n" +
                "  id BIGINT,\n" +
                "   user_id BIGINT,\n" +
                "   create_time STRING,\n" +
                "   operate_time STRING,\n" +
                "   province_id INT,\n" +
                "   order_status INT,\n" +
                "   total_amount DOUBLE,\n" +
                "   op STRING \n" +
                ") PARTITIONED BY (dt STRING, hr STRING) STORED AS parquet TBLPROPERTIES (\n" +
                "  'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00',\n" +
                "  'sink.partition-commit.trigger'='partition-time',\n" +
                "  'sink.partition-commit.delay'='1 min',\n" +
                "'sink.partition-commit.policy.kind'='metastore,success-file'\n" +
                ")");
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.executeSql("INSERT INTO ods.order_info\n" +
                        "SELECT \n" +
                        "id,\n" +
                        "user_id,\n" +
                        "create_time,\n" +
                        "operate_time,\n" +
                        "province_id,\n" +
                        "order_status,\n" +
                        "total_amount,\n" +
                        "op,\n" +
                        "DATE_FORMAT(TO_TIMESTAMP(create_time,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd') as dt,\n" +
                        "DATE_FORMAT(TO_TIMESTAMP(create_time,'yyyy-MM-ddHH:mm:ss'),'HH') as hr\n" +
                        "FROM merged_order_info");

        env.execute("flink-cdc");
    }
}
