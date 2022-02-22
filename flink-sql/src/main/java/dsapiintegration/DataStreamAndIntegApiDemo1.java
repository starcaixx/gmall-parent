package dsapiintegration;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.ZoneId;

public class DataStreamAndIntegApiDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //所有配置项尽可能在转换流之前配置好 // set various configuration early
//        env.setMaxParallelism(256);
//        env.getConfig().addDefaultKryoSerializer(MyCustom.class,CustemKryoSerializer.class);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //指定模式为批处理模式
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // then switch to Java Table API
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //批处理另外一种设置方式
//        StreamTableEnvironment.create(env, EnvironmentSettings.inBatchMode());
        // set configuration early
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("Europe/Berlin"));

        /*
        //chapter 1
        DataStreamSource<String> dataDS = env.fromElements("alice", "bob", "john");
        Table inputTable = tableEnv.fromDataStream(dataDS);

        tableEnv.createTemporaryView("inputTable",inputTable);

        Table resultTable = tableEnv.sqlQuery("select upper(f0) from inputTable");
        DataStream<Row> resultDS = tableEnv.toDataStream(resultTable);

        resultDS.print();
        */

        //chapter 2 写入有变动时 批/流处理模式
        /*
        DataStreamSource<Row> rowDS = env.fromElements(
                Row.of("alice", 12),
                Row.of("bob", 10),
                Row.of("alice", 100)
        );

        Table inputTable = tableEnv.fromDataStream(rowDS).as("name", "score");
        tableEnv.createTemporaryView("inputTable",inputTable);
        Table resultTable = tableEnv.sqlQuery("select name,sum(score) from inputTable group by name");
        DataStream<Row> resultDS = tableEnv.toChangelogStream(resultTable);
        //这个在流模式下会报错
//        DataStream<Row> resultDS = tableEnv.toDataStream(resultTable);
        resultDS.print();
        */

        //We recommend setting all configuration options in DataStream API early before switching to Table API.

        //exe to sink
        //execute with explicit sink
        /*
        tableEnv.from("inputTable").executeInsert("outputTable");
        tableEnv.executeSql("insert into outputTable select * from inputTable");
        tableEnv.createStatementSet()
                .addInsert("outputTable",tableEnv.from("inputTable"))
                .addInsert("outputTable2",tableEnv.from("inputTable"))
                .execute();

        tableEnv.createStatementSet()
                .addInsertSql("insert into ouputTable select * from inputTable")
                .addInsertSql("insert into outputTable2 select * from inputTable")
                .execute();

        // execute with implicit local sink
        tableEnv.from("inputTable").execute().print();
        tableEnv.executeSql("select * from inputTable").print();
        */
        Table table = tableEnv.from(TableDescriptor.forConnector("datagen")
                .option("number-of-rows", "10")
                .schema(Schema.newBuilder()
                        .column("uid", DataTypes.TINYINT())
                        .column("payload", DataTypes.STRING()).build())
                .build());

        // table 2 stream
        DataStream<Row> rowDataStream = tableEnv.toDataStream(table);

        SingleOutputStreamOperator<String> map = rowDataStream.keyBy(r -> r.getField("uid"))
                .map(r -> "custom:" + r.getField("payload"));
//                .executeAndCollect()
//                .forEachRemaining(System.out::println);

//        map.print();
        map.executeAndCollect().forEachRemaining(s->System.out.println(s));

        //flink任务中只能有一个最终的执行算子  executeAndCollect 也是一个执行算子
//        env.execute();
    }
}
