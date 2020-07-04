package com.codejianhongxie.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Metric implements Cloneable {

    private static Map<String, Number> counter = new ConcurrentHashMap<String, Number>();
    static {
        counter.put(Constants.BEGIN_TIMESTAMP, System.currentTimeMillis());
    }

    public synchronized static long getTimestamp() {
        Number value = counter.get(Constants.BEGIN_TIMESTAMP);
        return value == null ? 0 : value.longValue();
    }

    public synchronized static void setTimestamp(long timestamp) {
        counter.put(Constants.BEGIN_TIMESTAMP, timestamp);
    }

    public synchronized static long getReadCount() {
        Number value = counter.get(Constants.READ_COUNT);
        return value == null ? 0 : value.longValue();
    }

    public synchronized static void increaseReadCount(long readCount) {
        long value = getReadCount();
        counter.put(Constants.READ_COUNT, value + readCount);
    }

    public synchronized static long getWriteCount() {
        Number value = counter.get(Constants.WRITE_COUNT);
        return value == null ? 0 : value.longValue();
    }

    public synchronized static void increaseWriteCount(long writeCount) {
        long value = getWriteCount();
        counter.put(Constants.WRITE_COUNT, value + writeCount);
    }

    public synchronized static long getFailedCount() {
        Number value = counter.get(Constants.FAILED_COUNT);
        return value == null ? 0 : value.longValue();
    }

    public synchronized static void increaseFailedCount(long failedCount) {
        long value = getFailedCount();
        counter.put(Constants.FAILED_COUNT, value + failedCount);
    }
    public synchronized static long getExistedCount() {
        Number value = counter.get(Constants.EXISTED_COUNT);
        return value == null ? 0 : value.longValue();
    }

    public synchronized static void increaseExistedCount(long existedCount) {
        long value = getExistedCount();
        counter.put(Constants.EXISTED_COUNT, value + existedCount);
    }

    public synchronized static long getNotExistedCount() {
        Number value = counter.get(Constants.NOT_EXISTED_COUNT);
        return value == null ? 0 : value.longValue();
    }

    public synchronized static void increaseNotExistedCount(long existedCount) {
        long value = getNotExistedCount();
        counter.put(Constants.NOT_EXISTED_COUNT, value + existedCount);
    }

    public synchronized static long getTransferSpeed() {
        Number value = counter.get(Constants.TRANSFER_COUNT);
        return value == null ? 0 : value.longValue();
    }

    public synchronized static void increaseTransferSpeed(long transferSpeed) {
        long value = getTransferSpeed();
        counter.put(Constants.TRANSFER_COUNT, value + transferSpeed);
    }
}
