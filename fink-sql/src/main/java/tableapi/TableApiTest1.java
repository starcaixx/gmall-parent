package tableapi;

import org.apache.calcite.schema.TableFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;
import scala.Tuple3;

import java.time.LocalDateTime;

import static org.apache.flink.table.api.Expressions.*;

public class TableApiTest1 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
//        TableEnvironment tEnv = TableEnvironment.create(settings);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Row> orderDS = env.fromElements(
                Row.of(LocalDateTime.parse("2022-02-11T10:02:00"), 1, 122),
                Row.of(LocalDateTime.parse("2022-02-11T10:07:00"), 2, 239),
                Row.of(LocalDateTime.parse("2022-02-11T10:11:00"), 2, 999)
        ).returns(Types.ROW_NAMED(
                new String[]{"ts", "uid", "amount"},
                Types.LOCAL_DATE_TIME, Types.INT, Types.INT
        ));

        tableEnv.createTemporaryView("orders",orderDS, Schema.newBuilder()
                .column("ts", DataTypes.TIMESTAMP(3))
                .column("uid",DataTypes.INT())
                .column("amount",DataTypes.INT())
                .watermark("ts","ts-interval '1' second").build());

        Table orders = tableEnv.from("orders");

        Table counts = orders.groupBy($("uid"))
                .select($("uid"), $("amount").count().as("cnt"));
        counts.execute().print();

//        orders.where($("ts").isNotNull())
        orders.where(and($("ts").isNotNull(),$("uid").isNotNull()));
        Table result = orders.filter(and($("a").isNotNull(), $("b").isNotNull(), $("c").isNotNull()))
                .select($("a").lowerCase().as("a"), $("b"), $("rowtime"))
                .window(Tumble.over(lit(1).hours()).on($("rowtime")).as("hourlywindow"))
                .groupBy($("hourlywindow"), $("a"))
                .select($("a"), $("hourlywindow").end().as("hour"),
                        $("b").avg().as("avgBillingAmount"));

        //column f0 f1
        Table table0 = tableEnv.fromValues(
                row(1, "abc"),
                row(2, "abec")
        );

        //column id name
        Table table1 = tableEnv.fromValues(DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
                DataTypes.FIELD("name", DataTypes.STRING())
                ),
                row(1, "abc"),
                row(2, "abcde")
        );

        //通配符*选择所有列
        Table select = table1.select($("*"));

        //重命名字段名
        Table as = table0.as("id,name");


        Table where = table1.where($("b").isEqual("red"));
        //和fileter一样
        Table filter = table1.filter($("b").isEqual("red"));
        //添加列
        table1.addColumns(concat($("c"),"sunny"));
        //添加或者替换
        table1.addOrReplaceColumns(concat($("c"),"sunny").as("desc"));
        table1.dropColumns($("b"),$("c"));
        table1.renameColumns($("b").as("b2"),$("c").as("c2"));

        //对于流式查询，计算查询结果所需的状态可能会无限增长，具体取决于聚合类型和不同分组键的数量。请提供空闲状态保留时间以防止状态大小过大。有关详细信息，请参阅空闲状态保留时间。
        table1.groupBy($("a")).select($("a"),$("b").sum().as("d"));

        table1.window(Tumble.over(lit(5).minutes()).on($("rowtime")).as("w"))
                .groupBy($("a"),$("w"))
                .select($("a"),$("w").start(),
                        $("w").end(),$("w").rowtime(),
                        $("b").sum().as("d"));

        table1.window(Over.partitionBy($("a")).orderBy($("rowtime"))
        .preceding(UNBOUNDED_RANGE).following(CURRENT_RANGE).as("w"))
                .select($("a"),
                        $("b").avg().over($("w")),
                        $("b").max().over($("w")));

        Table groupbyDistinctResult = table1.groupBy($("a"))
                .select($("a"), $("b").sum().distinct().as("d"));
        Table groupbyWindowDistinctResult = table1.window(Tumble.over(lit(5).minutes()).on($("rowtime")).as("w"))
                .groupBy($("a"), $("w"))
                .select($("a"), $("b").sum().distinct().as("d"));
        Table result1 = table1.window(Over.partitionBy($("a")).orderBy($("rowtime"))
                .preceding(UNBOUNDED_RANGE).as("w"))
                .select($("a"), $("b").avg().distinct().over($("w")),
                        $("b").max().over($("w")),
                        $("b").min().over($("w")));

//        tableEnv.registerFunction("myUdagg",new MyUdagg());
        //函数调用
        table1.groupBy("users")
                .select($("users"),call("myUdagg",$("points")).distinct().as("myDistinctResult"));

        table1.distinct();
        Table left = tableEnv.from("MyTable").select($("a"), $("b"), $("c"));
        Table right = tableEnv.from("MyTable").select($("d"), $("e"), $("f"));
        left.join(right).where($("a").isEqual($("d")))
                .select($("a"),$("b"),$("e"));

        Table leftOutResult = left.leftOuterJoin(right, $("a").isEqual("d"))
                .select($("a"), $("b"), $("e"));
        Table rightOutResult = left.rightOuterJoin(right, $("a").isEqual($("d")))
                .select($("a"), $("b"), $("e"));
        Table fullOutResult = left.fullOuterJoin(right, $("a").isEqual($("d")))
                .select($("a"), $("b"), $("e"));

        Table left1 = tableEnv.from("MyTable").select($("a"), $("b"), $("c"), $("ltime"));
        Table right1 = tableEnv.from("MyTable").select($("d"), $("e"), $("f"), $("rtime"));
        left1.join(right1)
                .where(and(
                        $("a").isEqual($("d")),
                        $("ltime").isGreaterOrEqual($("rtime").minus(lit(5).minutes())),
                        $("ltime").isLess($("rtime").plus(lit(10).minutes()))
                ))
                .select($("a"),$("b"),$("e"),$("ltime"));

        //inner join udtf
//        TableFunction<Tuple3<String,String,String>> split = new MySplitUDTF();

//        tableEnv.registerCatalog("split",split);
        table1.joinLateral(call("split",$("c")).as("s","t","v"))
                .select($("a"),$("b"),$("s"),$("t"),$("v"));

        //left join udtf
        Table result2 = table1.leftOuterJoinLateral(call("split", $("c")).as("s", "t", "v"))
                .select($("a"), $("b"), $("s"), $("t"), $("v"));
        //join with temporal table
        Table ratesHistory = tableEnv.from("ratesHistory");
        TemporalTableFunction rates = ratesHistory.createTemporalTableFunction("r_proctime", "r_currency");
        tableEnv.registerFunction("rages",rates);
        Table result3 = table1.joinLateral(call("rates", $("o_proctime")), $("o_currency").isEqual($("r_currency")));

//        union
        left.union(right);
        //union all
        left.unionAll(right);
        //Intersect batch; The INTERSECT operation on two unbounded tables is currently not supported.
        left.intersect(right); //交集 去重后的

        //intersect all
        left.intersectAll(right);//未去重的交集

        //EXCEPT a中排除b中出现的
        //minus
        left.minus(right);
        //minusall
        left.minusAll(right);

        //in
        left.select($("a"),$("b"),$("c")).where($("a").in(right));

        //orderby offset fetch
        table1.orderBy($("a").asc());
        table1.orderBy($("a").asc()).fetch(5);
        table1.orderBy($("a").asc()).offset(3).fetch(10);

        //insert
        table1.executeInsert("outOrders");

        //grouop window
//        Table table = input
//                .window([GroupWindow w].as("w"))  // define window with alias w
//                .groupBy($("w"))  // group the table by window w
//                .select($("b").sum());  // aggregate

    }
}
