package com.star.gmall.realtime.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ThreadPoolUtil {
    public static ThreadPoolExecutor pool;

    /*
    获取单例的线程池对象
    corepoolsize：指定了线程池中的线程数量，他的数量决定了添加的任务是开辟新的线程去执行还是放到workqueue任务队列中去；
    maximumpoolsize：指定了线程池中的最大线程数量，这个参数会根据你使用的workqueue任务队列的类型，决定线程池会开辟的最大线程数量
    keepalivetime：但线程池中空闲线程数量超过corepoolsize时，多余的线程会再多长时间内被销毁；
    unit：keepalivetime的单位
    workqueue：任务队列，被添加到线程池中，但尚未被执行的任务
     */

    private static Lock lock = new ReentrantLock();

    public static ThreadPoolExecutor getInstance() {
        /*try{
            if (pool == null) {
                lock.lock();
                if (pool == null) {
                    pool = new ThreadPoolExecutor(4,20,300, TimeUnit.SECONDS,
                            new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE));
                }
            }
        } finally{
            lock.unlock();
        }*/

        if (pool == null) {
            synchronized (ThreadPoolUtil.class) {
                if (pool == null) {
                    System.out.println("create thread pool!");
                    pool = new ThreadPoolExecutor(4, 20, 300, TimeUnit.SECONDS,
                            new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE));
                }
            }
        }
        return pool;
    }
}
