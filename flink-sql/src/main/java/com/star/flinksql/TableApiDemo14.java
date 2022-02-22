package com.star.flinksql;

import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.table.api.*;

public class TableApiDemo14 {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        tableEnv.createTemporaryTable("sourceTable", TableDescriptor.forConnector("datagen")
                .schema(Schema.newBuilder().column("f0", DataTypes.STRING()).build())
                .option(DataGenConnectorOptions.ROWS_PER_SECOND,100l).build());

        tableEnv.executeSql("create temporary table sinkTable with ('connector'='blackhole') like sourcetable");

        Table table2 = tableEnv.from("sourcetable");
        Table table3 = tableEnv.sqlQuery("select * from sourcetable");

        TableResult tableResult = table2.executeInsert("sinktable");
        tableResult.print();
    }
}
