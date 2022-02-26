package dsapiintegration;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Instant;

import static org.apache.flink.table.api.Expressions.$;

public class FromDataStreamTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<User> userDS = env.fromElements(
                new User("Alice", 4, Instant.ofEpochMilli(1000)),
                new User("Bob", 6, Instant.ofEpochMilli(1001)),
                new User("Alice", 10, Instant.ofEpochMilli(1002))
        );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        /*
        //example 1
        //自动产生所有物理列
        Table table = tableEnv.fromDataStream(userDS);
        table.printSchema();

        //example2
        //自动产生物理列，并且增加计算列（衍生）
        Table table1 = tableEnv.fromDataStream(userDS,
                Schema.newBuilder()
                        .columnByExpression("proc_time", "proctime()").build());
        table1.printSchema();

        //example3
        //自动产生物理列，并且增加计算列和自定义水印
        Table table3 = tableEnv.fromDataStream(userDS,
                Schema.newBuilder()
                        .columnByExpression("rowtime", "cast(event_time as timestamp_ltz(3))")
                        .watermark("rowtime", "rowtime-interval '10' second")
                        .build());

        table3.printSchema();

        //example4
        //自动产生物理列，创建行记录时间戳，依赖ds上的水印
        Table table4 = tableEnv.fromDataStream(userDS,
                Schema.newBuilder()
                        .columnByMetadata("rowtime", "timestamp_ltz(3)")
                        .watermark("rowtime", "source_watermark()").build());
        table4.printSchema();

        //example5
        //手工指定物理列
        Table table5 = tableEnv.fromDataStream(userDS, Schema.newBuilder()
                .column("event_time", "timestamp_ltz(3)")
                .column("name", "string")
                .column("score", "int")
                .watermark("event_time", "SOURCE_WATERMARK()")
                .build());
        table5.printSchema();
        */

        //默认原生的类型是一个单列为f0的Raw类型的记录,如果是非pojo类型，该测试需要user为非标准的pojo，
        // 注释掉无参构造即可
        Table table6 = tableEnv.fromDataStream(userDS,Schema.newBuilder()
                .column("f0", DataTypes.of(User.class))
                .build())
                .as("user");
        table6.printSchema();

        Table table7 = tableEnv.fromDataStream(userDS, Schema.newBuilder()
                .column("f0", DataTypes.STRUCTURED(User.class,
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("score", DataTypes.INT())))
                .build())
                .as("user");
        table7.printSchema();
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class User{
        public String name;
        public Integer score;
        public Instant event_time;
    }
}


