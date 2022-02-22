package datastream.flink.dsapi;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class DataStreamDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Tuple2<Long, Long>> average = env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L))
                .keyBy(value -> value.f0)
                .flatMap(new RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
                    private transient ValueState<Tuple2<Long, Long>> sum;

                    @Override
                    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {
                        Tuple2<Long, Long> currentSum = sum.value();

                        currentSum.f0 += 1;
                        currentSum.f1 += input.f1;

                        sum.update(currentSum);

                        if (currentSum.f0 >= 2) {
                            out.collect(new Tuple2<>(input.f0, currentSum.f1 / currentSum.f0));
                            sum.clear();
                        }
                    }

                    @Override
                    public void open(Configuration parameters) {
                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(1))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
//                                .disableCleanupInBackground() //禁止清理状态后端
//                                .cleanupFullSnapshot() //不适用于 RocksDB 状态后端的增量检查点
//                                .cleanupIncrementally(10,true) //默认(5,false) 对RocksDB不适用
//                                .cleanupInRocksdbCompactFilter(1000)//压缩过滤器进行后台清理
                                .build();
                        ValueStateDescriptor<Tuple2<Long, Long>> average = new ValueStateDescriptor<>(
                                "average",
                                TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}),
                                Tuple2.of(0l,0l));// // default value of the state, if nothing was set,这个方法可能变了，如果去掉默认值则会报错

                        sum = getRuntimeContext().getState(average);
                        average.enableTimeToLive(ttlConfig);
                    }
                });

        average.print();
        //watermark
        /*
        //kafka source
        KafkaDeserializationSchema<String> schema= new KafkaDeserializationSchema<String>() {
            @Override
            public boolean isEndOfStream(String nextElement) {
                return false;
            }

            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                return null;
            }

            @Override
            public TypeInformation<String> getProducedType() {
                return null;
            }
        };
        Properties props = new Properties();
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>("topic", schema, props);

        kafkaSource.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)));
        env.addSource(kafkaSource);
        */
        /*
        //simple example
        ArrayList<Person> people = new ArrayList<>();
        DataStreamSource<Person> peopleDS = env.fromCollection(people);

        DataStream<Person> flintstones = env.fromElements(
                new Person("Wilma", 35),
                new Person("Fred", 35),
                new Person("Pebbles", 2));

        SingleOutputStreamOperator<Person> adult = flintstones.filter(per -> {
            return per.age > 18;
        });

        adult.print();*/

        /*
        *//**
         * 迭代流
         *//*
        DataStreamSource<Long> randomDs = env.generateSequence(1, 3);

        IterativeStream<Long> iterate = randomDs.iterate();
        SingleOutputStreamOperator<Long> minusone = iterate.map(value -> value - 1);
        minusone.print();

        SingleOutputStreamOperator<Long> filter = minusone.filter(value -> value > 0);
        iterate.closeWith(filter);
        SingleOutputStreamOperator<Long> lessthanzero = minusone.filter(value -> value <= 0);*/
        env.execute();
    }

    public static class Person {
        public String name;
        public Integer age;
        public Person() {};

        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        }

        public String toString() {
            return this.name.toString() + ": age " + this.age.toString();
        }
    }
}
