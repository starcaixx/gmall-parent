package tableapi;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;

import static org.apache.flink.table.api.Expressions.*;

public class TableApiTest2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        /*EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        Table tmp = tableEnv.fromValues(DataTypes.ROW(
                DataTypes.FIELD("uid", DataTypes.DECIMAL(10, 2)),
                DataTypes.FIELD("name", DataTypes.STRING()),
                DataTypes.FIELD("ts",DataTypes.TIMESTAMP(3))
                ),
                row(1, "abcde0",LocalDateTime.parse("2022-02-11T10:02:00")),
                row(1, "abcde1",LocalDateTime.parse("2022-02-11T10:02:01")),
                row(3, "abcde2",LocalDateTime.parse("2022-02-11T10:02:02")),
                row(6, "abcde3",LocalDateTime.parse("2022-02-11T10:02:03")),
                row(6, "abcde4",LocalDateTime.parse("2022-02-11T10:02:04")),
                row(6, "abcde5",LocalDateTime.parse("2022-02-11T10:02:05")),
                row(7, "abcde6",LocalDateTime.parse("2022-02-11T10:02:06")),
                row(8, "abcde7",LocalDateTime.parse("2022-02-11T10:02:07")),
                row(9, "abcde8",LocalDateTime.parse("2022-02-11T10:02:08"))
        );*/

        SingleOutputStreamOperator<Row> orderDS = env.fromElements(
                Row.of(LocalDateTime.parse("2022-02-11T10:02:00"), 1, 122),
                Row.of(LocalDateTime.parse("2022-02-11T10:02:01"), 1, 22),
                Row.of(LocalDateTime.parse("2022-02-11T10:07:02"), 3, 239),
                Row.of(LocalDateTime.parse("2022-02-11T10:11:03"), 6, 499),
                Row.of(LocalDateTime.parse("2022-02-11T10:11:04"), 6, 299),
                Row.of(LocalDateTime.parse("2022-02-11T10:11:05"), 6, 199),
                Row.of(LocalDateTime.parse("2022-02-11T10:11:06"), 7, 799),
                Row.of(LocalDateTime.parse("2022-02-11T10:11:07"), 8, 999),
                Row.of(LocalDateTime.parse("2022-02-11T10:11:08"), 9, 899)
        ).returns(org.apache.flink.api.common.typeinfo.Types.ROW_NAMED(
                new String[]{"ts", "uid", "amount"},
                org.apache.flink.api.common.typeinfo.Types.LOCAL_DATE_TIME, org.apache.flink.api.common.typeinfo.Types.INT, Types.INT
        ));


        tableEnv.createTemporaryView("orders",orderDS, Schema.newBuilder()
                .column("ts", DataTypes.TIMESTAMP(3))
                .column("uid",DataTypes.INT())
                .column("amount",DataTypes.INT())
                .watermark("ts","ts-interval '1' second")
                .build());

        Table orders = tableEnv.from("orders");

        orders.printSchema();
        Table select = orders.window(
//                Over.orderBy($("ts")).as("w"),
//                Over.orderBy($("ts")).preceding(UNBOUNDED_RANGE).as("w_range")
//                Over.orderBy($("ts")).preceding(UNBOUNDED_ROW).as("w_row")
//                Over.orderBy($("ts")).preceding("1.rows").following("2.rows").as("w_row_line")
                Over.partitionBy($("a")).orderBy($("rowtime")).preceding(lit(1).minutes()).as("w"),
                Over.partitionBy($("a")).orderBy($("rowtime")).preceding(rowInterval(10l)).as("w")
//                Over.orderBy($("ts")).preceding().following(lit(2)).as("w_rrange_line")
        )
                .select($("uid"),
//                        $("uid").sum().over($("w")).as("default_sum"),
//                        $("uid").sum().over($("w_range")).as("range_sum")
//                        $("uid").sum().over($("w_row")).as("row_sum")
                        $("uid").sum().over($("w_row_line")).as("row_sum1")
                );
        select.execute().print();


        //flinksql 性能优化
        //mini batch
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.mini-batch.enabled", "true"); // enable mini-batch optimization
        configuration.setString("table.exec.mini-batch.allow-latency", "5 s"); // use 5 seconds to buffer input records
        configuration.setString("table.exec.mini-batch.size", "5000"); // the maximum number of records can be buffered by each aggregate operator task

        //combine reduce 理解为预聚合 并不适应所有情况
        configuration.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE"); // enable two-phase, i.e. local-global aggregation
        //拆分 聚合操作 可以理解为抽样后随机分桶，需要结合具体数据分析 随机打乱,加盐操作等等
        /*
        select day,count(distinct user_id) from t group by day
         */
        /*
        select day,sum(cnt) from (select day,count(distinct user_id)as cnt from t GROUP BY day,mod(hash_code(user_id),1024))t1 group by day
         */
        configuration.setString("table.optimizer.distinct-agg.split.enabled", "true");  // enable distinct agg split

        //使用filter作为过滤条件替换case when
        /*
        select
        day,count(distinct user_id) as total_uv,
        count(distinct case when flag in ('android','iphone') then user_id else null end) as app_uv,
        count(distinct case when flag in ('wap','other') then user_id else null end) as web_uv from t group by day
         */
        /*
        select
        day,
        count(distinct user_id) as total_uv,
        count(distinct user_id) filter (where flag in ('android','iphone')) as app_uv,
        count(distinct user_id) filter (where flag in ('wap','other')) as web_uv
        from t group by day
         */
    }
}
