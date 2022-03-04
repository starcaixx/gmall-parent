import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.table.api.*;

public class TableApiDemo14 {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        tableEnv.createTemporaryTable("sourceTable", TableDescriptor.forConnector("datagen")
                .schema(Schema.newBuilder().column("f0", DataTypes.STRING()).build())
                .option(DataGenConnectorOptions.ROWS_PER_SECOND,10l).build());

        tableEnv.executeSql("CREATE TEMPORARY TABLE SinkTable WITH ('connector' = 'none') LIKE sourceTable");
//        tableEnv.executeSql("CREATE TEMPORARY TABLE SinkTable WITH ('connector' = 'blackhole') LIKE sourceTable");

        Table table2 = tableEnv.from("sourceTable");
        Table table3 = tableEnv.sqlQuery("select * from sourceTable");

        TableResult tableResult = table2.executeInsert("SinkTable");
        tableResult.print();
    }
}
