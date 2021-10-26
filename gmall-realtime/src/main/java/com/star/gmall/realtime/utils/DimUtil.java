package com.star.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;

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
        String sql = "select * from " + tableName+whereSql;
        System.out.println("sql:>>>:"+sql);

        JSONObject dimInfoJsonObj = null;
        List<JSONObject> dimList = PhoenixUtil.queryList(sql, JSONObject.class);
        if (dimList != null && dimList.size() > 0) {
            //因为关联维度，肯定都是根据key关联得到一条记录
            dimInfoJsonObj = dimList.get(0);
        } else {
            System.out.println("no dim data:"+sql);
        }
        return dimInfoJsonObj;
    }

    public static void main(String[] args) {
        JSONObject dimInfoNoCache = getDimInfoNoCache("base_trademark", Tuple2.of("id", "13"));
        System.out.println(dimInfoNoCache);
    }
}
