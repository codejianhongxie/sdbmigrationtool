package com.codejianhongxie.executor;


import com.codejianhongxie.util.Constants;
import com.codejianhongxie.util.Metric;
import com.codejianhongxie.util.Stringify;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricThread implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(MetricThread.class);
    private String operation;
    private boolean isLob;
    public MetricThread(String operation, boolean isLob) {
        this.operation = operation;
        this.isLob = isLob;
    }
    @Override
    public void run() {

        if (isLob) {
            if (this.operation.equals(Constants.DEFAULT_MIGRATION_TYPE)) {
                printMigrationLobMetric();
            } else if (this.operation.equals(Constants.DEFAULT_VALIDATE_TYPE)) {
                printValidateLobMetric();
            } else if (this.operation.equals(Constants.DEFAULT_REPAIR_TYPE)) {
                printRepairLobMetric();
            }
        } else {
            if (this.operation.equals(Constants.DEFAULT_MIGRATION_TYPE)) {
                printMigrationDataMetric();
            }
        }

    }

    private void printRepairLobMetric() {
        long timeInterval = System.currentTimeMillis() - Metric.getTimestamp();
        long sec = timeInterval <= 1000 ? 1 : timeInterval / 1000;
        long bytesSpeed = Metric.getTransferSpeed() / sec;
        String readSpeed = Stringify.format(Metric.getReadCount() / (double)sec, "/s");
        String writeSpeed = Stringify.format(Metric.getWriteCount() / (double)sec, "/s");
        String transferSpeed = Stringify.stringify(bytesSpeed < 0 ? 0 : bytesSpeed,"/s");
        String metric = String.format(
                "%s:%9s" +
                        "%s:%10s" +
                        "%s:%10s" +
                        "%s:%10s" +
                        "%s:%8s" +
                        "%s:%8s" ,
                "finished record",Metric.getWriteCount(),
                ",read",readSpeed,
                ", write",writeSpeed,
                ", speed",transferSpeed,
                ", existed",Metric.getExistedCount(),
                ", failed",Metric.getFailedCount());
        logger.info(metric);
    }

    private void printValidateLobMetric() {
        long timeInterval = System.currentTimeMillis() - Metric.getTimestamp();
        long sec = timeInterval <= 1000 ? 1 : timeInterval / 1000;
        long bytesSpeed = Metric.getTransferSpeed() / sec;
        String readSpeed = Stringify.format(Metric.getReadCount() / (double)sec, "/s");
        String writeSpeed = Stringify.format(Metric.getWriteCount() / (double)sec, "/s");
        String transferSpeed = Stringify.stringify(bytesSpeed < 0 ? 0 : bytesSpeed,"/s");
        String metric = String.format(
                "%s:%9s" +
                        "%s:%10s" +
                        "%s:%10s" +
                        "%s:%10s" +
                        "%s:%8s" +
                        "%s:%8s" ,
                "validated record",Metric.getWriteCount(),
                ",read",readSpeed,
                ", validate",writeSpeed,
                ", existed",Metric.getExistedCount(),
                ", not existed",Metric.getNotExistedCount(),
                ", failed",Metric.getFailedCount());
        logger.info(metric);
    }

    private void printMigrationLobMetric() {
        long timeInterval = System.currentTimeMillis() - Metric.getTimestamp();
        long sec = timeInterval <= 1000 ? 1 : timeInterval / 1000;
        long bytesSpeed = Metric.getTransferSpeed() / sec;
        String readSpeed = Stringify.format(Metric.getReadCount() / (double)sec, "/s");
        String writeSpeed = Stringify.format(Metric.getWriteCount() / (double)sec, "/s");
        String transferSpeed = Stringify.stringify(bytesSpeed < 0 ? 0 : bytesSpeed,"/s");
        String metric = String.format(
                "%s:%9s" +
                        "%s:%10s" +
                        "%s:%10s" +
                        "%s:%10s" +
                        "%s:%8s" +
                        "%s:%8s" ,
                "finished record",Metric.getWriteCount(),
                ",read",readSpeed,
                ", write",writeSpeed,
                ", speed",transferSpeed,
                ", existed",Metric.getExistedCount(),
                ", failed",Metric.getFailedCount());
        logger.info(metric);
    }

    private void printMigrationDataMetric() {
        long timeInterval = System.currentTimeMillis() - Metric.getTimestamp();
        long sec = timeInterval <= 1000 ? 1 : timeInterval / 1000;
        long totalWriteRecord = Metric.getWriteCount();
        long totalReadRecord = Metric.getReadCount();
        String readSpeed = Stringify.format(totalReadRecord / sec, "/s");
        String writeSpeed = Stringify.format(totalWriteRecord / sec, "/s");
        String metric = String.format(
                "%s:%9s" +
                        "%s:%10s" +
                        "%s:%10s",
                "finished record", totalWriteRecord,
                ",read",readSpeed,
                ", write",writeSpeed);
        logger.info(metric);
    }
}
