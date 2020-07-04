package com.codejianhongxie.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author xiejianhong
 * @description
 * @date 2020/7/1 19:56
 */
public class ThreadExecutorFactory {

    public ExecutorService getExecutor(String executorName, int maximumPoolSize, int corePoolSize, int queueSize) {

        return new ThreadPoolExecutor(
                        corePoolSize,
                        maximumPoolSize,
                        60000,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<Runnable>(queueSize),
                        new MigrationThreadFactory(executorName),
                new ThreadPoolExecutor.AbortPolicy());

    }
}
