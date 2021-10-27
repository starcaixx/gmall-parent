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
        env.setStateBackend(new FsStateBackend("hdfs://node:9000/gmall/checkpoint"));

        //重启策略
        //如果没有开启checkpoint，则重启策略为norestart
        //如果开启了checkpoint，则重启策略默认为自动帮你重试，会重试Integer.maxvalue次,这里重试可以理解为算子出错重试次数
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


        final OutputTag<JSONObject> outputHbaseTag = new OutputTag<JSONObject>("side-output-"+TableProcess.SINK_TYPE_HBASE){};
//        final OutputTag<String> outputClickHouseTag = new OutputTag<String>("side-output-ck"){};
        SingleOutputStreamOperator<JSONObject> kafkaDs = filterDs.process(new ProcessFunction<JSONObject, JSONObject>() {

            private Map<String,TableProcess> configMapping= new HashMap<>();
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
                    if (configMapping.containsKey(table)) {
                        TableProcess tableProcess = configMapping.get(table);
                        String sinkTable = tableProcess.getSinkTable();
                        value.put("sink_table",sinkTable);
                        String sinkType = tableProcess.getSinkType();
                        String columns = tableProcess.getSinkColumns();
                        if (columns != null && !"".equals(columns)) {
                            System.out.println(data);
                            Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();
                            while (iterator.hasNext()) {
                                Map.Entry<String, Object> next = iterator.next();
                                if (!columns.contains(next.getKey())) {
                                    //test concurrentException
                                    data.remove(next.getKey());
                                    iterator.remove();
                                }
                            }
                        }

                        if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                            ctx.output(outputHbaseTag, value);
                        } else if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
                            out.collect(value);
                        }else{
                            //todo: send to other
                        }

                    } else {
                        System.out.println("no this table config");
                    }

                } else {
                    System.out.println("配置信息为空，请检查！");
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

            //每个并行度执行一次
            @Override
            public void open(Configuration parameters) throws Exception {
//                lastTimestamp = getRuntimeContext().getState(new ValueStateDescriptor<>("timerState", Long.class));
                Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
                connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
                refreshMetadata();

                //method 2 1min后每10分钟执行一次，定时器，周期性读取配置信息
                Timer timer = new Timer();
                timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        System.out.println(":::"+System.currentTimeMillis());refreshMetadata();
                    }
                }, 1000 * 60, 1000 * 60 * 5);
            }

            private void refreshMetadata() {
                List<TableProcess> tableProcesses = MySQLUtil.queryList("select * from table_process", TableProcess.class, true);
                if (tableProcesses == null || tableProcesses.size() == 0) {
                    throw new RuntimeException("无配置信息");
                }
                for (TableProcess tableProcess : tableProcesses) {
//                    String operateType = tableProcess.getOperateType();
                    String sourceTable = tableProcess.getSourceTable();
                    configMapping.put(sourceTable, tableProcess);

                    //如果表不存在hbase中则新建表
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

                //主键不存在,则给定默认值
                if (sinkPk == null) {
                    sinkPk = "id";
                }
                //扩展字段不存在,则给定默认值
                if (ext == null) {
                    ext = "";
                }
                //创建字符串拼接对象,用于拼接建表语句SQL
                StringBuilder createSql = new StringBuilder("create table if not exists " + GmallConfig.HBASE_SCHEMA + "." + tableName + "(");
                //将列做切分,并拼接至建表语句SQL中
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
                    String upSql = genUpsertSql(sink_table.toUpperCase(),data);
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

                if (data.getString("type").equals("update") || "delete".equals(data.getString("type"))) {
                    DimUtil.deleteCached(sink_table,data.getString("id"));
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

            //从每条数据得到该条数据应送往的主题名
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                String sink_table = element.getString("sink_table");
                JSONObject data = element.getJSONObject("data");
                return new ProducerRecord<>(sink_table,data.toJSONString().getBytes());
            }
        }));

        env.execute();
    }


}
