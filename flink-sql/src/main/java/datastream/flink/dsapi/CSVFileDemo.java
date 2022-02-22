package datastream.flink.dsapi;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import scala.util.parsing.json.JSON;

public class CSVFileDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String path = "E:\\test";
        TextInputFormat format = new TextInputFormat(new Path(path));
        format.setFilesFilter(FilePathFilter.createDefaultFilter());
        TypeInformation<String> typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
        format.setCharsetName("UTF-8");
        DataStreamSource fileDS = env.readFile(format, path, FileProcessingMode.PROCESS_CONTINUOUSLY, 10L, (TypeInformation) typeInfo);

        /*SingleOutputStreamOperator<JSONObject> jsonDS = fileDS.map(str -> {
            JSONObject jsonObject = JSON.parseObject(str.toString());
            return jsonObject;
        });

        SingleOutputStreamOperator<JSONObject> compDS = jsonDS.filter(json -> {
            String doc_type = StringUtils.join(json.getObject("doc_type", String[].class));
            return doc_type.contains("company");
        });

        SingleOutputStreamOperator<JSONObject> streamOperator = compDS.assignTimestampsAndWatermarks(new WatermarkStrategy<JSONObject>() {
            @Override
            public WatermarkGenerator<JSONObject> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                *//*
                //有界无序水印生成器
                return new WatermarkGenerator<JSONObject>() {

                    private final long maxOutOfOrderness = 3500;
                    private long currentMaxTimestamp;

                    @Override
                    public void onEvent(JSONObject jsonObject, long l, WatermarkOutput watermarkOutput) {
                        currentMaxTimestamp = Math.max(currentMaxTimestamp, l);
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                        watermarkOutput.emitWatermark(new Watermark(currentMaxTimestamp-maxOutOfOrderness-1));
                    }
                };*//*

                //时差水印生成器
                return new WatermarkGenerator<JSONObject>() {
                    private final long maxTimelag = 5000;

                    @Override
                    public void onEvent(JSONObject jsonObject, long l, WatermarkOutput watermarkOutput) {
                        // don't need to do anything because we work on processing time
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                        watermarkOutput.emitWatermark(new Watermark(System.currentTimeMillis() - maxTimelag));
                    }
                };

                //标点分配器
                *//*return new WatermarkGenerator<JSONObject>() {
                    @Override
                    public void onEvent(JSONObject event, long l, WatermarkOutput watermarkOutput) {
                        if (event.hasWatermarkMarker()) {
                            watermarkOutput.emitWatermark(new Watermark(event.getWatermarkTimestamp()));
                        }
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                        // don't need to do anything because we emit in reaction to events above
                    }
                };*//*
            }
        });*/


//        jsonDS.print();

//        DataStreamSource<String> fileDS = env.readTextFile("E:\\test");
//        fileDS.print();
//        String path = "E:\\test\\csv";
//        TypeInformation[] fieldTypeInfos = new TypeInformation[3];
//        TypeInformation<JSONObject>[] typeInformations = new TypeInformation<JSONObject>[1];
//        fieldTypeInfos[0] = TypeInformation.of(String.class);
//        fieldTypeInfos[1] = TypeInformation.of(String.class);
//        fieldTypeInfos[2] = TypeInformation.of(String.class);

//        DataStreamSource<Row> csvDS = env.readFile(new RowCsvInputFormat(new Path(path), fieldTypeInfos), path, FileProcessingMode.PROCESS_ONCE, -1L);

//        csvDS.print();
        env.execute();
    }
}
