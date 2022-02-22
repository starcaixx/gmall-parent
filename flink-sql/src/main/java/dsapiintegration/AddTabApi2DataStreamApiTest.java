package dsapiintegration;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class AddTabApi2DataStreamApiTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        StreamStatementSet statementSet = tabEnv.createStatementSet();

        //create source
        TableDescriptor sourceDescriptor = TableDescriptor.forConnector("datagen")
                .option("number-of-rows", "3")
                .schema(Schema.newBuilder()
                        .column("myCol", DataTypes.INT())
                        .column("otherCol", DataTypes.BOOLEAN())
                        .build()).build();
        //create sink
        TableDescriptor sinkDescriptor = TableDescriptor.forConnector("print").build();
        Table tableFromSource = tabEnv.from(sourceDescriptor);

        //add table api pipiline
        statementSet.addInsert(sinkDescriptor,tableFromSource);

        //add stream
        DataStreamSource<Integer> dataStream = env.fromElements(1, 2, 3);
        Table tableFromStream = tabEnv.fromDataStream(dataStream);
        statementSet.addInsert(sinkDescriptor,tableFromStream);

        statementSet.attachAsDataStream();

        //定义其他ds
//        env.fromElements(4,5,6).addSink(new DiscardingSink<>());
        env.execute();
    }
}
