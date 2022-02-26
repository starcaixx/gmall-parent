package datastream.flink.dsapi;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class SocketDSDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 8888);

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> window = socketDS.flatMap((String str, Collector<Tuple2<String, Integer>> out) -> {
            for (String word : str.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        /*SingleOutputStreamOperator<Tuple2<String, Integer>> sumDS = socketDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String str : s.split(" ")) {
                    collector.collect(new Tuple2<String, Integer>(str, 1));
                }
            }
        }).keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1);*/

        /*window.apply(new WindowFunction<Tuple2<String,Integer>, Integer, Tuple, Window>() {
            public void apply (Tuple tuple,
                               Window window,
                               Iterable<Tuple2<String, Integer>> values,
                               Collector<Integer> out) throws Exception {
                int sum = 0;
                for (Tuple2<String, Integer> value : values) {
                    sum += value.f1;
                }
                out.collect (new Integer(sum));
            }
        });*/

        window.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return new Tuple2<>(t1.f0,t1.f1+t2.f1);
            }
        }).print();
//        sumDS.print();


        env.execute("window WC");
    }
}
