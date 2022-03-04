package datastream.flink.dsapi;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.source.TransactionSource;

import java.io.IOException;

public class FraudDetectionJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Transaction> transactions = env.addSource(new TransactionSource())
                .name("transactions");

        DataStream<Alert> alerts = transactions.keyBy(Transaction::getAccountId)
                .process(new KeyedProcessFunction<Long, Transaction, Alert>() {
                    private transient ValueState<Boolean> lastAmt;
                    private transient ValueState<Long> timerState;
                    private static final double SMALL_AMOUNT = 1.00;
                    private static final double LARGE_AMOUNT = 500.00;
                    private static final long ONE_MINUTE = 60 * 1000;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Boolean> lastTradeAmt = new ValueStateDescriptor<>("lastTradeAmt", Types.BOOLEAN()/*Double.TYPE*/);
                        lastAmt = getRuntimeContext().getState(lastTradeAmt);

                        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>("timer-state", Types.LONG());
                        timerState=getRuntimeContext().getState(timerDescriptor);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        timerState.clear();
                        lastAmt.clear();
                    }

                    private void cleanUp(Context ctx) throws IOException {
                        Long timer = timerState.value();
                        if (timer != null) {
                            ctx.timerService().deleteProcessingTimeTimer(timer);
                        }

                        timerState.clear();
                        lastAmt.clear();
                    }

                    @Override
                    public void processElement(Transaction transaction, Context context, Collector<Alert> collector) throws Exception {
                        Boolean lstAmt = lastAmt.value();
                        double crtAmt = transaction.getAmount();

                        if (crtAmt > LARGE_AMOUNT && lstAmt!=null && lstAmt) {
                            System.out.println(lstAmt+":::"+crtAmt);
                            Alert alert = new Alert();
                            alert.setId(transaction.getAccountId());
                            collector.collect(alert);
                        }

                        cleanUp(context);

                        if (transaction.getAmount() < SMALL_AMOUNT) {
                            lastAmt.update(true);

                            long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
                            timerState.update(timer);
                        }
                    }
                })
                .name("fraud-detector");

        alerts.addSink(new AlertSink())
                .name("send-alert");

        alerts.print();

        env.execute("fraud-detection");
    }
}
