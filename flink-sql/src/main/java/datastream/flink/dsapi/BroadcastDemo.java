package datastream.flink.dsapi;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.calcite.shaded.org.apache.commons.codec.language.bm.Rule;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class BroadcastDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Integer>> tpDs = env.fromElements(
                Tuple2.of("halef", 1),
                Tuple2.of("abae", 1),
                Tuple2.of("feaf", 1),
                Tuple2.of("hafealef", 1),
                Tuple2.of("fae", 1),
                Tuple2.of("fa", 1),
                Tuple2.of("fasaf", 1)
        );

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tpDs.keyBy(tp -> tp.f0);

        MapStateDescriptor<String, Rule> broadcastdemo = new MapStateDescriptor<>("broadcastdemo", BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<Rule>() {
                }));

        /*BroadcastStream<Rule> ruleBroadcastStream = ruleStream
                .broadcast(broadcastdemo);

        tpDs.connect(ruleBroadcastStream)
                // type arguments in our KeyedBroadcastProcessFunction represent:
                //   1. the key of the keyed stream
                //   2. the type of elements in the non-broadcast side
                //   3. the type of elements in the broadcast side
                //   4. the type of the result, here a string
                .process(new KeyedBroadcastProcessFunction<String, Tuple2<String,Integer>, Rule, String>() {

                    // store partial matches, i.e. first elements of the pair waiting for their second element
                    // we keep a list as we may have many first elements waiting
                    private final MapStateDescriptor<String,List<Tuple2<String,Integer>>> mapStateDesc =
                            new MapStateDescriptor<String, List<Tuple2<String, Integer>>>("items",
                                    BasicTypeInfo.STRING_TYPE_INFO,
                                    new ListTypeInfo<>(Types.TUPLE(Types.STRING,Types.INT)));


                    // identical to our ruleStateDescriptor above
                    private final MapStateDescriptor<String,Rule> ruleStateDescriptor =
                            new MapStateDescriptor<String, Rule>("ruleBroadcastState",
                                    BasicTypeInfo.STRING_TYPE_INFO,
                                    TypeInformation.of(new TypeHint<Rule>() {}));

                    @Override
                    public void processElement(Tuple2<String, Integer> stringIntegerTuple2, ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
                        final MapState<String, List<Tuple2<String, Integer>>> mapState = getRuntimeContext().getMapState(mapStateDesc);
                        String key = stringIntegerTuple2.f0;

                        for (Map.Entry<String, Rule> ruleEntry : readOnlyContext.getBroadcastState(ruleStateDescriptor).immutableEntries()) {
                            String ruleName = ruleEntry.getKey();
                            Rule ruleValue = ruleEntry.getValue();
                            List<Tuple2<String, Integer>> stored = mapState.get(ruleName);
                            if (stored == null) {
                                stored = new ArrayList<>();
                            }
                            //业务逻辑
                        }

                    }

                    @Override
                    public void processBroadcastElement(Rule rule, Context context, Collector<String> collector) throws Exception {
                        context.getBroadcastState(ruleStateDescriptor).put("rule",rule);
                    }
                });
*/

        env.execute();

    }
}
