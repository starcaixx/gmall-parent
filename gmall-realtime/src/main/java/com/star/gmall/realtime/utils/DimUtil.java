package com.star.gmall.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

public class DimUtil {

    //直接从phoenix查询
    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String, String>... colNameAndValue) {
        String whereSql = new String(" where ");
        for (int i = 0; i < colNameAndValue.length; i++) {
            //获取查询列名以及对应的值
            Tuple2<String, String> nameValueTuple = colNameAndValue[i];
            String fieldName = nameValueTuple.f0;
            String fileldValue = nameValueTuple.f1;

            if (i > 0) {
                whereSql += " and ";
            }

            whereSql += fieldName + "='" + fileldValue + "'";
        }

        //组合查询sql
        String sql = "select * from " + tableName + whereSql;
        System.out.println("sql:>>>:" + sql);

        JSONObject dimInfoJsonObj = null;
        List<JSONObject> dimList = PhoenixUtil.queryList(sql, JSONObject.class);
        if (dimList != null && dimList.size() > 0) {
            //因为关联维度，肯定都是根据key关联得到一条记录
            dimInfoJsonObj = dimList.get(0);
        } else {
            System.out.println("no dim data:" + sql);
        }
        return dimInfoJsonObj;
    }

    public static JSONObject getDimInfo(String tableName, String id) {
        Tuple2<String, String> kv = Tuple2.of("id", id);
        return getDimInfo(tableName, kv);
    }

    /**
     * key  dim:tableName:字段  eg:dim:DIM_BASE_TRADEMARK:10_XX
     * value 查询结果转换后的json
     *
     * @param tableName
     * @param colNameAndValue
     * @return
     */
    public static JSONObject getDimInfo(String tableName, Tuple2<String, String>... colNameAndValue) {
        String whereSql = " where ";
        String redisKey = "";
        for (int i = 0; i < colNameAndValue.length; i++) {
            Tuple2<String, String> nameValueTuple = colNameAndValue[i];
            String fieldName = nameValueTuple.f0;
            String fieldValue = nameValueTuple.f1;
            if (i > 0) {
                whereSql += " and ";
                redisKey += "_";
            }

            whereSql += fieldName + "='" + fieldValue + "'";
            redisKey += fieldValue;
        }

        Jedis jedis = null;
        String dimJson = null;

        JSONObject dimInfo = null;
        String key = "dim:" + tableName.toLowerCase() + ":" + redisKey;
        try {
            jedis = RedisUtil.getJedis();
            dimJson = jedis.get(key);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (dimJson != null) {
            dimInfo = JSON.parseObject(dimJson);
        } else {
            String sql = "select * from " + tableName + whereSql;
            System.out.println("query sql:" + sql);

            List<JSONObject> dimList = PhoenixUtil.queryList(sql, JSONObject.class);
            if (dimList.size() > 0) {
                dimInfo = dimList.get(0);
                if (jedis != null) {
                    jedis.setex(key, 3600 * 24, dimInfo.toJSONString());
                }
            } else {
                System.out.println("no data be fund:" + sql);
            }
        }
        if (jedis != null) {
            jedis.close();
            System.out.println("close jedis conn");
        }

        return dimInfo;
    }

    public static void deleteCached(String tableName, String id) {
        String key = "dim:" + tableName.toLowerCase() + ":" + id;
        try {
            Jedis jedis = RedisUtil.getJedis();
            jedis.del(key);
            jedis.close();
        } catch (Exception e) {
            System.out.println("cached exception!");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        JSONObject dimInfoNoCache = getDimInfoNoCache("base_trademark", Tuple2.of("id", "13"));
        System.out.println(dimInfoNoCache);
    }
}
