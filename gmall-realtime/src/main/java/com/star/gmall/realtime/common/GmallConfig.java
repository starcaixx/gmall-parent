package com.star.gmall.realtime.common;

public class GmallConfig {
    public static final String HBASE_SCHEMA = "DATAPLAT_REALTIME";
//    public static final String HBASE_SCHEMA = "GMALL_REALTIME";
    public static final String PHOENIX_SERVER = "jdbc:phoenix:node:2181";
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://node:8123/default";
}
