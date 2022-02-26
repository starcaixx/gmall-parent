package dsapiintegration;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TemporaryViewTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<Long, String>> tpDS = env.fromElements(
                Tuple2.of(12l, "Alice"),
                Tuple2.of(0l, "Bob")
        );
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //example1 注册ds2view，列自动生成
        tableEnv.createTemporaryView("view",tpDS);
        tableEnv.from("view").printSchema();


        //example2 注册ds2view，指定schema信息
        tableEnv.createTemporaryView("view1",tpDS, Schema.newBuilder()
                .column("f0","bigint")
                .column("f1","string")
                .build());
        tableEnv.from("view1").printSchema();

        //example3 使用table api创建view
        tableEnv.createTemporaryView("view2",tableEnv.fromDataStream(tpDS).as("id","name"));
        tableEnv.from("view2").printSchema();
    }
}
