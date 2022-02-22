import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.functions.ScalarFunction;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class SpendReport {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tabEnv = TableEnvironment.create(settings);

        tabEnv.executeSql("create table transactions(" +
                " account_id bigint," +
                " amount bigint," +
                " transaction_time timestamp(3)," +
                " watermark for transaction_time as transaction_time - interval '5' second" +
                ") with ('connector' = 'kafka'," +
                " 'topic'='transactions'," +
                " 'properties.bootstrap.servers'='kafka;9092'," +
                " 'format'='csv')");

        tabEnv.executeSql("create table spend_report (" +
                " account_id bigint," +
                " log_ts timestamp(3)," +
                " amount bigint," +
                " primary key (account_id,log_ts) not enforced) with (" +
                " 'connector'='jdbc'," +
                " 'url'='jdbc:mysql://mysql:3306/sql-demo'," +
                " 'table-name'='spend_report'," +
                " 'driver'='com.mysql.jdbc.Driver'," +
                " 'username'='sql-demo'," +
                " 'password'='demo-sql'," +
                ")");

        Table transactions = tabEnv.from("transactions");
        report(transactions).executeInsert("spend_report");
    }

    private static Table report(Table transactions) {
//        return transactions.select($("account_id"),
//                $("transaction_time").floor(TimeIntervalUnit.HOUR).as("log_ts"),
//                $("amount"))
//                .groupBy($("account_id"),$("log_ts"))
//                .select($("account_id"),$("log_ts"),$("amount").sum().as("amount"));

//        return transactions.select($("account_id"),
//                call(MyFloor.class,$("transaction_time")).as("log_ts"),
//                $("amount"))
//                .groupBy($("account_id"),$("log_ts"))
//                .select($("account_id"),$("log_ts"),
//                        $("amount").sum().as("amount"));

        return transactions.window(Tumble.over(lit(1).hour()).on($("transaction_time")).as("log_ts"))
                .groupBy($("account_id"),$("log_ts"))
                .select(
                        $("account_id"),
                        $("log_ts").start().as("log_ts"),
                        $("amount").sum().as("amount")
                );
    }

    private static class MyFloor extends ScalarFunction{
        public @DataTypeHint("TIMESTAMP(3)") LocalDateTime eval(@DataTypeHint("TIMESTAMP(3)") LocalDateTime timestamp) {
            return timestamp.truncatedTo(ChronoUnit.HOURS);
        }
    }
}
