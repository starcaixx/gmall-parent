package dsapiintegration;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;

import static org.apache.flink.table.api.Expressions.$;

public class ChangeLogUnite {
    public static void main(String[] args) throws Exception {
        //设置ds api
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置batch模式
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        //设置table api
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //创建user ds
        SingleOutputStreamOperator<Row> userDS = env.fromElements(
                Row.of(LocalDateTime.parse("2022-02-11T10:00:00"), 1, "Alice"),
                Row.of(LocalDateTime.parse("2022-02-11T10:05:00"), 2, "Bob"),
                Row.of(LocalDateTime.parse("2022-02-11T10:10:00"), 2, "Bob")
        ).returns(Types.ROW_NAMED(
                new String[]{"ts", "uid", "name"},
                Types.LOCAL_DATE_TIME, Types.INT, Types.STRING
        ));

        //创建order ds
        SingleOutputStreamOperator<Row> orderDS = env.fromElements(
                Row.of(LocalDateTime.parse("2022-02-11T10:02:00"), 1, 122),
                Row.of(LocalDateTime.parse("2022-02-11T10:07:00"), 2, 239),
                Row.of(LocalDateTime.parse("2022-02-11T10:11:00"), 2, 999)
        ).returns(Types.ROW_NAMED(
                new String[]{"ts", "uid", "amount"},
                Types.LOCAL_DATE_TIME, Types.INT, Types.INT
        ));

        //创建对应的表
        tableEnv.createTemporaryView("userTable",userDS, Schema.newBuilder()
                .column("ts", DataTypes.TIMESTAMP(3))
                .column("uid",DataTypes.INT())
                .column("name",DataTypes.STRING())
                .watermark("ts","ts-interval '1' second")
                .build());

        tableEnv.createTemporaryView("orderTable",orderDS, Schema.newBuilder()
                .column("ts", DataTypes.TIMESTAMP(3))
                .column("uid",DataTypes.INT())
                .column("amount",DataTypes.INT())
                .watermark("ts","ts-interval '1' second").build());

        //内联
        Table joinedTable = tableEnv.sqlQuery(
                "select \n" +
                        "u.name,o.amount\n" +
                        "from userTable u join orderTable o \n" +
                        "on u.uid = o.uid where o.ts between u.ts and u.ts+interval '5' minutes"
        );
        DataStream<Row> joinedDS = tableEnv.toDataStream(joinedTable);
        joinedDS.print();

        joinedTable.orderBy($("name").asc()).fetch(1).execute().print();
        joinedTable.orderBy($("name").asc()).offset(1).fetch(2).execute().print();
        joinedTable.execute().print();
        joinedTable.intersect(joinedTable).execute().print();
        joinedTable.minusAll(joinedTable.where($("name").isEqual("Alice")).select($("*"))).execute().print();

        joinedDS.keyBy(r->r.<String>getFieldAs("name"))
                .process(new KeyedProcessFunction<String, Row, String>() {
                    ValueState<String> seen;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        seen = getRuntimeContext().getState(new ValueStateDescriptor<String>("seen",String.class));
                    }

                    @Override
                    public void processElement(Row value, Context ctx, Collector<String> out) throws Exception {
                        String name = value.<String>getFieldAs("name");
                        if (seen.value() == null) {
                            seen.update(name);
                            out.collect(name);
                        }
                    }
                }).print();
        env.execute();
    }
}
