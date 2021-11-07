package com.star.gmall.realtime.utils;

import com.star.gmall.realtime.common.GmallConfig;
import net.minidev.json.JSONObject;
import org.apache.commons.beanutils.BeanUtils;
import org.codehaus.jackson.map.util.BeanUtil;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PhoenixUtil {
    public static Connection conn = null;

    public static void queryInit() {
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            conn.setSchema(GmallConfig.HBASE_SCHEMA);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static <T> List<T> queryList(String sql,Class<T> clazz) {
        if (conn == null) {
            queryInit();
        }

        ArrayList<T> resultList = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            ResultSetMetaData md = rs.getMetaData();
            while (rs.next()) {
                T rowData = clazz.newInstance();
                for (int i = 1; i < md.getColumnCount(); i++) {
                    BeanUtils.setProperty(rowData,md.getColumnName(i),rs.getObject(i));
                }
                resultList.add(rowData);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        return resultList;
    }


    public static void main(String[] args) {
        List<JSONObject> jsonObjects = queryList("select * from base_trademark", JSONObject.class);
        System.out.println(jsonObjects);
    }
}
