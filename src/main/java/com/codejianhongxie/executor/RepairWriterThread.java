package com.codejianhongxie.executor;

import com.codejianhongxie.exchanger.BsonRecordData;
import com.codejianhongxie.exchanger.BufferedExchanger;
import com.codejianhongxie.exchanger.Record;
import com.codejianhongxie.exchanger.TerminalRecord;
import com.codejianhongxie.po.SdbConnectionInfo;
import com.codejianhongxie.writer.RepairLobWriter;
import com.codejianhongxie.writer.Writer;
import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author xiejianhong
 * @description
 * @date 2020/6/30 19:33
 */
public class RepairWriterThread implements Runnable {

    private final static Logger logger = LoggerFactory.getLogger(RepairWriterThread.class);
    private Writer writer;
    private BufferedExchanger<Record<BSONObject>> bufferedExchanger;
    public RepairWriterThread(BufferedExchanger<Record<BSONObject>> bufferedExchanger,
                              SdbConnectionInfo sourceConnection, SdbConnectionInfo dstConnection, int maxRate) {
        this.bufferedExchanger = bufferedExchanger;
        writer = new RepairLobWriter(sourceConnection, dstConnection, maxRate);
    }


    @SuppressWarnings("unchecked")
    @Override
    public void run() {

        while(true) {

            Record<BSONObject> recordData = null;
            try {
                recordData = bufferedExchanger.take();

                if (recordData instanceof TerminalRecord) {
                    bufferedExchanger.put(recordData);
                    break;
                }
                this.writer.process((BsonRecordData<BSONObject>) recordData);
            } catch (Exception e) {
                logger.error("数据校验写线程{}发生异常, 线程退出", Thread.currentThread().getName(), e);
                break;
            }
        }
        if (writer != null) {
            writer.close();
        }
    }
}
