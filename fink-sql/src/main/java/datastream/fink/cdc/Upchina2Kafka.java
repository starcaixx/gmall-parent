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
import datastream.util.MyKafkaUtil;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nullable;

public class Upchina2Kafka {
    /*public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

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

        DebeziumSourceFunction mysqlSource = MySqlSource.<ObjectNode>builder()
                .hostname("rm-bp146k5r8kw09t21xho.mysql.rds.aliyuncs.com")
//                .hostname("47.94.1.2")
//                .hostname("bj-cynosdbmysql-grp-rzunn0bc.sql.tencentcdb.com")
                .port(3306)
//                .port(20452)
//                .username("mysql")
                .username("star_test")
//                .password("mysql")
//                .password("3iu8dn4H#2JD1")
                .password("STARcai01230")
                .databaseList("canal_test")
//                .databaseList("upchina")
                .startupOptions(StartupOptions.initial())
                .deserializer(new DebeziumDeserializationSchema() {

                    @Override
                    public TypeInformation getProducedType() {
                        return TypeInformation.of(ObjectNode.class);
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
                        System.out.println("value:" + value);

                        String ts_ms = value.get("ts_ms").toString();
                        //获取变化后的数据
                        Struct after = value.getStruct("after");

                        ObjectNode data = objectMapper.createObjectNode();

                        for (Field field : after.schema().fields()) {
                            Object o = after.get(field);
                            data.put(field.name(), o == null ? null : o.toString());
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

                        System.out.println("result:" + result);
                        //发送数据至下游
                        collector.collect(objectNode);
                    }
                })
                .build();

//        DataStreamSource mysqlDS = env.fromSource(mysqlSource, WatermarkStrategy.forMonotonousTimestamps(), "mysql56");
        DataStreamSource<ObjectNode> mysqlDS = env.addSource(mysqlSource);

        mysqlDS.print(">>>>>>>>>>>>>>>>>>>>");

        mysqlDS.addSink(MyKafkaUtil.getKafkaSinkbySchema(new KafkaSerializationSchema<ObjectNode>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(ObjectNode element, @Nullable Long timestamp) {
                String sink_table = element.get("table").asText();
                JsonNode data = element.get("data");
                System.out.println("kafka talbe:"+sink_table+":data:"+data.toString());
                return new ProducerRecord<>(sink_table,data.toString().getBytes());
            }
        }));
        env.execute("flink-cdc-kafka");
    }*/

    //注意版本关系 https://ververica.github.io/flink-cdc-connectors/release-2.0/content/about.html

    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
        //2.1 开启Checkpoint,每隔5秒钟做一次CK
        env.enableCheckpointing(5000L);
        //2.2 指定CK的一致性语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //2.3 设置任务关闭的时候保留最后一次CK数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 指定从CK自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
        //2.5 设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flinkCDC"));

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

        //3.创建Flink-MySQL-CDC的Source
        //initial (default): Performs an initial snapshot on the monitored database tables upon first startup, and continue to read the latest binlog.
        //latest-offset: Never to perform snapshot on the monitored database tables upon first startup, just read from the end of the binlog which means only have the changes since the connector was started.
        //timestamp: Never to perform snapshot on the monitored database tables upon first startup, and directly read binlog from the specified timestamp. The consumer will traverse the binlog from the beginning and ignore change events whose timestamp is smaller than the specified timestamp.
        //specific-offset: Never to perform snapshot on the monitored database tables upon first startup, and directly read binlog from the specified offset.
        DebeziumSourceFunction<ObjectNode> mysqlSource = MySqlSource.<ObjectNode>builder()
//                .hostname("rm-bp146k5r8kw09t21xho.mysql.rds.aliyuncs.com")
                .hostname("47.94.1.2")
//                .hostname("bj-cynosdbmysql-grp-rzunn0bc.sql.tencentcdb.com")
                .port(3306)
//                .port(20452)
                .username("mysql")
//                .username("star_test")
                .password("mysql")
//                .password("3iu8dn4H#2JD")
//                .password("STARcai01230")
//                .databaseList("gmall")
                .databaseList("upchina")
                .startupOptions(StartupOptions.initial())
                .deserializer(new DebeziumDeserializationSchema() {

                    @Override
                    public TypeInformation getProducedType() {
                        return TypeInformation.of(ObjectNode.class);
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
                        System.out.println("value:" + value);

                        String ts_ms = value.get("ts_ms").toString();
                        //获取变化后的数据
                        Struct after = value.getStruct("after");

                        ObjectNode data = objectMapper.createObjectNode();

                        for (Field field : after.schema().fields()) {
                            Object o = after.get(field);
                            data.put(field.name(), o == null ? null : o.toString());
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

                        System.out.println("result:" + result);
                        //发送数据至下游
                        collector.collect(objectNode);
                    }
                })
//                .deserializer(new StringDebeziumDeserializationSchema())
                .build();

        //4.使用CDC Source从MySQL读取数据
        DataStreamSource<ObjectNode> mysqlDS = env.addSource(mysqlSource);

        //5.打印数据
        mysqlDS.print(">>>>>>>>>>>>>>>>>>>>");

        mysqlDS.addSink(MyKafkaUtil.getKafkaSinkbySchema(new KafkaSerializationSchema<ObjectNode>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(ObjectNode element, @Nullable Long timestamp) {
                String sink_table = element.get("table").asText();
                JsonNode data = element.get("data");
                System.out.println("kafka talbe:"+sink_table+":data:"+data.toString());
                return new ProducerRecord<>(sink_table,data.toString().getBytes());
            }
        }));

        //6.执行任务
        env.execute("flink-cdc-kafka");

    }
}
