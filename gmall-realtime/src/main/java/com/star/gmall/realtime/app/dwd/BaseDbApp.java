package com.star.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.star.gmall.realtime.bean.TableProcess;
import com.star.gmall.realtime.common.GmallConfig;
import com.star.gmall.realtime.utils.DimUtil;
import com.star.gmall.realtime.utils.MyKafkaUtil;
import com.star.gmall.realtime.utils.MySQLUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;

public class BaseDbApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://node:8020/gmall/dwdbasedbapp/checkpoint"));

        //????????????
        //??????????????????checkpoint?????????????????????norestart
        //???????????????checkpoint?????????????????????????????????????????????????????????Integer.maxvalue???,???????????????????????????????????????????????????
//        env.setRestartStrategy(RestartStrategies.noRestart());

        DataStreamSource<String> dbDs = env.addSource(MyKafkaUtil.getKafkaSource("ods_base_db_m", "con_db_group"));

        SingleOutputStreamOperator<JSONObject> mapDs = dbDs.map(str -> {
            return JSON.parseObject(str);
        });

        SingleOutputStreamOperator<JSONObject> filterDs = mapDs.filter(jsonObj -> {
            String table = jsonObj.getString("table");
            JSONObject data = jsonObj.getJSONObject("data");
            return table != null && !"".equals(table) && data != null && jsonObj.getString("data").length() > 3;
        });


        final OutputTag<JSONObject> outputHbaseTag = new OutputTag<JSONObject>("side-output-" + TableProcess.SINK_TYPE_HBASE) {
        };
//        final OutputTag<String> outputClickHouseTag = new OutputTag<String>("side-output-ck"){};
        SingleOutputStreamOperator<JSONObject> kafkaDs = filterDs.process(new ProcessFunction<JSONObject, JSONObject>() {

            private Map<String, TableProcess> configMapping = new HashMap<>();
            private HashSet<String> existsFlag = new HashSet<>();
            private Connection connection = null;

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                String table = value.getString("table");
                String type = value.getString("type");
                JSONObject data = value.getJSONObject("data");

                if ("bootstrap-insert".equals(type)) {
                    type = "insert";
                    value.put("type", type);
                }
                if (configMapping.size() > 0) {
                    String key = table + "|" + type;
                    if (configMapping.containsKey(key)) {
                        TableProcess tableProcess = configMapping.get(key);
                        String sinkTable = tableProcess.getSinkTable();
                        value.put("sink_table", sinkTable);
                        String sinkType = tableProcess.getSinkType();
                        String columns = tableProcess.getSinkColumns();
                        if (columns != null && !"".equals(columns)) {
//                            System.out.println(data);
                            Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();
                            while (iterator.hasNext()) {
                                Map.Entry<String, Object> next = iterator.next();
                                if (!columns.contains(next.getKey())) {
                                    //test concurrentException ConcurrentModificationException
                                    iterator.remove();
                                }
                            }
                        }

                        if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                            ctx.output(outputHbaseTag, value);
                        } else if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
                            out.collect(value);
                        } else {
                            //todo: send to other
                        }

                    } else {
                        System.out.println("no this table config");
                    }

                } else {
                    System.out.println("?????????????????????????????????");
                    System.exit(0);
                }

            }
/*
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                if (timestamp == lastTimestamp.value() + 60000 * 5) {
                    refreshMetadata();
                }

                lastTimestamp.update(timestamp);
            }*/

            //???????????????????????????
            @Override
            public void open(Configuration parameters) throws Exception {
//                lastTimestamp = getRuntimeContext().getState(new ValueStateDescriptor<>("timerState", Long.class));
                Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
                connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
                refreshMetadata();

                //method 2 1min??????10????????????????????????????????????????????????????????????
                Timer timer = new Timer();
                timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        System.out.println(":::" + System.currentTimeMillis());
                        refreshMetadata();
                    }
                }, 1000 * 60, 1000 * 60 * 5);
            }

            private void refreshMetadata() {
                List<TableProcess> tableProcesses = MySQLUtil.queryList("select * from table_process", TableProcess.class, true);
                if (tableProcesses == null || tableProcesses.size() == 0) {
                    throw new RuntimeException("???????????????");
                }
                for (TableProcess tableProcess : tableProcesses) {
                    String operateType = tableProcess.getOperateType();
                    String sourceTable = tableProcess.getSourceTable();
                    configMapping.put(sourceTable + "|" + operateType, tableProcess);

                    //??????????????????hbase???????????????
                    if ("hbase".equals(tableProcess.getSinkType()) && !existsFlag.contains(sourceTable)) {
                        String createTableSql = buildCreatSql(tableProcess);
                        System.out.println(createTableSql);

                        PreparedStatement ps = null;
                        try {
                            ps = connection.prepareStatement(createTableSql);
                            ps.execute();
                            ps.close();
                            existsFlag.add(sourceTable);
                        } catch (SQLException throwables) {
                            throwables.printStackTrace();
                        } finally {
                            if (ps != null) {
                                try {
                                    ps.close();
                                } catch (SQLException throwables) {
                                    throwables.printStackTrace();
                                }
                            }
                        }
                    }
                }
            }

            private String buildCreatSql(TableProcess tableProcess) {
                String sinkPk = tableProcess.getSinkPk();
                String ext = tableProcess.getSinkExtend();
                String tableName = tableProcess.getSinkTable();

                //???????????????,??????????????????
                if (sinkPk == null) {
                    sinkPk = "id";
                }
                //?????????????????????,??????????????????
                if (ext == null) {
                    ext = "";
                }
                //???????????????????????????,????????????????????????SQL
                StringBuilder createSql = new StringBuilder("create table if not exists " + GmallConfig.HBASE_SCHEMA + "." + tableName + "(");
                //???????????????,????????????????????????SQL???
                String[] fieldsArr = tableProcess.getSinkColumns().split(",");
                for (int i = 0; i < fieldsArr.length; i++) {
                    String field = fieldsArr[i];
                    if (sinkPk.equals(field)) {
                        createSql.append(field).append(" varchar primary key ");
                    } else {
                        createSql.append("info.").append(field).append(" varchar");
                    }
                    if (i < fieldsArr.length - 1) {
                        createSql.append(",");
                    }
                }
                createSql.append(")");
                createSql.append(ext);
                return createSql.toString();
            }
        });

        DataStream<JSONObject> hbaseDs = kafkaDs.getSideOutput(outputHbaseTag);
        kafkaDs.print("kafka>>>>");
        hbaseDs.print("hbase>>>>");

        hbaseDs.addSink(new RichSinkFunction<JSONObject>() {
            Connection conn = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
                conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
                super.open(parameters);
            }

            @Override
            public void invoke(JSONObject value, Context context) throws Exception {
                String sink_table = value.getString("sink_table");

                JSONObject data = value.getJSONObject("data");
                if (data != null && data.size() > 0) {
                    String upSql = genUpsertSql(sink_table.toUpperCase(), data);
                    System.out.println(upSql);
                    try {
                        PreparedStatement ps = conn.prepareStatement(upSql);
                        ps.executeUpdate();
                        conn.commit();
                        ps.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                String type = value.getString("type");
                System.out.println(type);
                if ("update".equals(type) || "delete".equals(type)) {
                    DimUtil.deleteCached(sink_table, data.getString("id"));
                }
            }

            public String genUpsertSql(String tableName, JSONObject jsonObject) {
                Set<String> fields = jsonObject.keySet();
                String upsertSql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + tableName + "(" + StringUtils.join(fields, ",") + ")";
                String valuesSql = " values ('" + StringUtils.join(jsonObject.values(), "','") + "')";
                return upsertSql + valuesSql;
            }

            @Override
            public void close() throws Exception {
                if (conn != null) {
                    conn.close();
                }
                super.close();
            }
        });

        kafkaDs.addSink(MyKafkaUtil.getKafkaSinkbySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public void open(SerializationSchema.InitializationContext context) throws Exception {
                System.out.println("start kafka sink");
            }

            //??????????????????????????????????????????????????????
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                String sink_table = element.getString("sink_table");
                JSONObject data = element.getJSONObject("data");
                System.out.println("kafka talbe:" + sink_table);
                return new ProducerRecord<>(sink_table, data.toJSONString().getBytes());
            }
        }));

        env.execute();
    }


}
