package dsapiintegration;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ToChangeLogStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql(
                "create table generatedTable(\n" +
                        "\tname string,\n" +
                        "\tscore int,\n" +
                        "\tevent_time timestamp_ltz(3),\n" +
                        "\twatermark for event_time as event_time-interval '10' second\n" +
                        ")with ('connector'='datagen')");

        Table table = tableEnv.from("generatedTable");
        table.execute().print();
        //example1 简单通用的方式转换（非事件时间）
        /*Table simpleTable = tableEnv.fromValues(row("Alice", 12), row("Alice", 2), row("Bob", 12))
                .as("name", "score")
                .groupBy($("name"))
                .select($("name"), $("score").sum());
        tableEnv.toChangelogStream(simpleTable)
                .executeAndCollect()
                .forEachRemaining(System.out::println);*/

        //example2 简单通用的方式（带事件时间）
        /*
        DataStream<Row> dataStream = tableEnv.toChangelogStream(table);
        dataStream.process(new ProcessFunction<Row, Void>() {
            @Override
            public void processElement(Row value, Context ctx, Collector<Void> out) throws Exception {
                System.out.println(value.getFieldNames(true));
                assert ctx.timestamp() == value.<Instant>getFieldAs("event_time").toEpochMilli();
                *//*if () {
                    System.out.println("hahah");
                    ;
                }*//*
            }
        });
        */

        //example3  转换时间属性到元数据列中
        /*
        DataStream<Row> dataStream = tableEnv.toChangelogStream(table, Schema.newBuilder()
                .column("name", "string")
                .column("score", "int")
                .columnByMetadata("rowtime", "timestamp_ltz(3)")
                .build());

        dataStream.process(new ProcessFunction<Row, Void>() {
            @Override
            public void processElement(Row value, Context ctx, Collector<Void> out) throws Exception {
                System.out.println(value.getFieldNames(true));
                System.out.println(ctx.timestamp());
            }
        });
*/
        //example4 更底层更灵活的操作
        // leads to a stream of Row(name: StringData, score: Integer, event_time: Long)
        /*DataStream<Row> dataStream = tableEnv.toChangelogStream(table, Schema.newBuilder()
                .column("name", DataTypes.STRING().bridgedTo(StringData.class))
                .column("score", DataTypes.INT())
                .column("event_time", DataTypes.TIMESTAMP_LTZ(3).bridgedTo(Long.class))
                .build());

        dataStream.process(new ProcessFunction<Row, Object>() {
            @Override
            public void processElement(Row value, Context ctx, Collector<Object> out) throws Exception {
                System.out.println(value.getFieldNames(true));
                System.out.println(ctx.timestamp());
            }
        });

        env.execute();*/
    }
}
