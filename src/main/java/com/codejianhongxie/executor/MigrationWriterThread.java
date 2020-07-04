package com.codejianhongxie.executor;

import com.codejianhongxie.exchanger.BsonRecordData;
import com.codejianhongxie.exchanger.BufferedExchanger;
import com.codejianhongxie.exchanger.Record;
import com.codejianhongxie.exchanger.TerminalRecord;
import com.codejianhongxie.po.SdbConnectionInfo;
import com.codejianhongxie.writer.MigrationDataWriter;
import com.codejianhongxie.writer.MigrationLobWriter;
import com.codejianhongxie.writer.Writer;
import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author xiejianhong
 * @description
 * @date 2020/6/30 19:33
 */
public class MigrationWriterThread implements Runnable {

    private final static Logger logger = LoggerFactory.getLogger(MigrationWriterThread.class);
    private Writer writer;
    private BufferedExchanger<Record<BSONObject>> bufferedExchanger;
    public MigrationWriterThread(BufferedExchanger<Record<BSONObject>> bufferedExchanger,
                                 SdbConnectionInfo sourceConnection, SdbConnectionInfo dstConnection, boolean isLob, int maxRate) {
        this.bufferedExchanger = bufferedExchanger;
        if (isLob) {
            writer = new MigrationLobWriter(bufferedExchanger, sourceConnection, dstConnection, maxRate);
        } else {
            writer = new MigrationDataWriter(bufferedExchanger, dstConnection);
        }
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
                logger.error("数据写线程{}发生异常, 线程退出", Thread.currentThread().getName(), e);
                break;
            }
        }

        if (writer != null) {
            writer.close();
        }
    }
}
