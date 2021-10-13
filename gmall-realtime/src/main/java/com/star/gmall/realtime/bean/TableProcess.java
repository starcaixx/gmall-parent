package com.star.gmall.realtime.bean;

import lombok.Data;

@Data
public class TableProcess {
    //动态分流Sink常量   改为小写和脚本一致
    public static final String SINK_TYPE_HBASE = "hbase";
    public static final String SINK_TYPE_KAFKA = "kafka";
    public static final String SINK_TYPE_CK = "clickhouse";

    private String sourceTable;
    private String operateType;
    private String sinkType;
    private String sinkTable;
    private String sinkColumns;
    private String sinkPk;
    private String sinkExtend;
}
