package dsapiintegration;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.time.Instant;

public class FromChangeLogStreamTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //example1
        DataStream<Row> dataStream =
                env.fromElements(
                        Row.ofKind(RowKind.INSERT, "Alice", 12),
                        Row.ofKind(RowKind.INSERT, "Bob", 5),
                        Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", 12),
                        Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table table = tableEnv.fromChangelogStream(dataStream);

        tableEnv.createTemporaryView("inputTable",table);
        tableEnv.executeSql("select f0 as name,sum(f1) as score from inputTable group by f0")
                .print();

        //example2
        DataStreamSource<Row> datastream1 = env.fromElements(
                Row.ofKind(RowKind.INSERT, "Alice", 12),
                Row.ofKind(RowKind.INSERT, "Bob", 5),
                Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100)
        );
        Table table1 = tableEnv.fromChangelogStream(datastream1, Schema.newBuilder()
                .primaryKey("f0").build(), ChangelogMode.upsert());
        tableEnv.createTemporaryView("inputTable1",table1);
        tableEnv.executeSql("select f0 as name,sum(f1) as score from inputTable1 group by f0")
                .print();
    }
}
