import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class TableApiTest1 {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
/*

        Table projTab = tEnv.from("").select("");
        tEnv.createTemporaryView("projectedTable",projTab);
        TableDescriptor sourceDescriptor = TableDescriptor.forConnector("datagen")
                .schema(Schema.newBuilder().column("f0", DataTypes.STRING()).build())
                .option(DataGenConnectorOptions.ROWS_PER_SECOND, 10L)
                .build();

        tEnv.createTable("sourceTableA",sourceDescriptor);
        tEnv.createTemporaryTable("sourceTableB",sourceDescriptor);

        tEnv.executeSql("create table mytable () with ()");

        tEnv.useCatalog("custom_catalog");
        tEnv.useDatabase("custom_database");

        tEnv.createTemporaryView("exampleView",projTab);
        tEnv.createTemporaryView("other_database.exampleview",projTab);
//        标识符遵循 SQL 要求，这意味着它们可以使用反引号字符 ( `) 进行转义
        tEnv.createTemporaryView("`example.View`", projTab);
        // register the view named 'exampleView' in the catalog named 'other_catalog'
// in the database named 'other_database'
        tEnv.createTemporaryView("other_catalog.other_database.exampleView",projTab);

        Table orders = tEnv.from("orders");
        //Table API 聚合查询
        Table revenue = orders.filter($("cCountry").isEqual("FRANCE"))
                .groupBy($("cID"), $("cName"))
                .select($("cID"), $("cName"), $("revenue").sum().as("revSum"));

        //指定查询并将结果作为Table
        Table revenueWithSQL = tEnv.sqlQuery(
                "select cID,cName,sum(revenue) as revSum from Orders where cCountry='FRANCE' group by cID,cName"
        );

//        指定将其结果插入已注册表的更新查询
        tEnv.executeSql(
                "insert into RevenueFrance select cID,cName,sum(revenue) as revSum" +
                        "from Orders where cCountry = 'FRANCE' group by cID,cName"
        );

        //send 2 sink
        Schema schema = Schema.newBuilder()
                .column("a", DataTypes.INT())
                .column("b", DataTypes.STRING())
                .column("c", DataTypes.BIGINT())
                .build();

        tEnv.createTemporaryTable("csvSinkTable",TableDescriptor.forConnector("filesystem")
        .schema(schema)
        .option("path","/path/to/file")
        .format(FormatDescriptor.forFormat("csv").option("field-delimiter","|").build())
        .build());

        tEnv.sqlQuery("xxx").executeInsert("csvSinkTable");*/


        //other methor
        /*StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tmpEnv = StreamTableEnvironment.create(env);

        DataStreamSource<Tuple2<Integer, String>> stream1 = env.fromElements(new Tuple2<>(1, "hello"));
        DataStreamSource<Tuple2<Integer, String>> stream2 = env.fromElements(new Tuple2<>(2, "word"));

        Table table1 = tmpEnv.fromDataStream(stream1, $("count"), $("word"));
        Table table2 = tmpEnv.fromDataStream(stream1, $("count"), $("word"));
        Table table = table1.where($("word").like("F%"))
                .unionAll(table2);

        System.out.println(table.explain());*/

        Schema schema = Schema.newBuilder().column("count", DataTypes.INT())
                .column("word", DataTypes.STRING())
                .build();

        tEnv.createTemporaryTable("mySource1",TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .option("path","/source/path1")
                .format("csv")
                .build());

        tEnv.createTemporaryTable("mySource2", TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .option("path","/source/path2")
                .format("csv").build());

        tEnv.createTemporaryTable("mySink1", TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .option("path", "/sink/path1")
                .format("csv")
                .build());
        tEnv.createTemporaryTable("mySink2", TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .option("path", "/sink/path2")
                .format("csv")
                .build());

        StatementSet stmtSet = tEnv.createStatementSet();
        Table table1 = tEnv.from("mySource1").where($("word").like("F%"));
        stmtSet.addInsert("mySink1",table1);

        Table table2 = table1.unionAll(tEnv.from("mySource2"));
        stmtSet.addInsert("mySink2",table2);
        String explain = stmtSet.explain();
        System.out.println(explain);


    }
}
