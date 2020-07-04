package com.codejianhongxie.executor;

import com.codejianhongxie.util.Constants;
import com.codejianhongxie.util.Metric;
import com.codejianhongxie.util.Stringify;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricExecutor implements Runnable {
    private final Logger logger = LoggerFactory.getLogger("console");
    private String operation;
    public MetricExecutor(String operation){
        this.operation = operation;
    }
    @Override
    public void run() {
        if(this.operation.equals(Constants.DEFAULT_MIGRATION_TYPE)){
            long timeInterval = System.currentTimeMillis() - Metric.getTimestamp();
            long sec = timeInterval <= 1000 ? 1 : timeInterval / 1000;
            long totalWriteRecord = Metric.getWriteCount();
            long totalReadRecord = Metric.getReadCount();
            String readSpeed = Stringify.format(totalReadRecord / (double)sec, "/s");
            String writeSpeed = Stringify.format(totalWriteRecord / (double)sec, "/s");
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
}
