package dsapiintegration;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Instant;

public class ToDataStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(
               "create table generatedTable (\n" +
                       "\tname string,\n" +
                       "\tscore int,\n" +
                       "\tevent_time timestamp_ltz(3),\n" +
                       "\twatermark for event_time as event_time - interval '10' second\n" +
                       ") with ('connector'='datagen')"
        );
        Table table = tableEnv.from("generatedTable");

        //example1 使用默认转换
        DataStream<Row> dataStream = tableEnv.toDataStream(table);
        //example2 从对象中提取类型
        DataStream<User> dataStream1 = tableEnv.toDataStream(table, User.class);

        DataStream<Object> dataStream2 = tableEnv.toDataStream(table, DataTypes.STRUCTURED(User.class,
                DataTypes.FIELD("name", DataTypes.STRING()),
                DataTypes.FIELD("score", DataTypes.INT()),
                DataTypes.FIELD("event_time", DataTypes.TIMESTAMP_LTZ(3))
        ));

        dataStream.print("default>>>");
        dataStream1.print("user>>>>>>>>");
        dataStream2.print("xx>>>>>>>>>>");
        env.execute();
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
