package com.star.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.star.gmall.realtime.utils.DimUtil;
import com.star.gmall.realtime.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.util.ThreadUtil;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;

/**
 * 自定义纬度查询异步执行函数
 * RichAsyncFunction 里面的方法负责异步查询
 * DimJoinFunction  里面的方法负责将维表和主表进行关联
 * @param <T>
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements  DimJoinFunction<T> {
    ExecutorService executorService = null;
    public String tableName = null;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("get thread pool!");
        executorService = ThreadPoolUtil.getInstance();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        executorService.submit(()->{
            try {
                long start = System.currentTimeMillis();

                //从流对象中获取主键
                String key = getKey(input);

                //根据主键获取纬度对象数据
                JSONObject dimJsonObj = DimUtil.getDimInfo(tableName, key);

                System.out.println("dimJsonObj::"+dimJsonObj);

                if (dimJsonObj != null) {
                    //纬度数据和流数据关联
                    join(input,dimJsonObj);
                }

                System.out.println("input:"+input);
                long end = System.currentTimeMillis();
                System.out.println("async cost "+(end - start) + "ms");

                resultFuture.complete(Arrays.asList(input));
            } catch (Exception e) {
                e.printStackTrace();
            }

        });
    }
}
