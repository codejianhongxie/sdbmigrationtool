package com.codejianhongxie.executor;

import com.codejianhongxie.exchanger.BsonRecordData;
import com.codejianhongxie.exchanger.BufferedExchanger;
import com.codejianhongxie.exchanger.Record;
import com.codejianhongxie.exchanger.TerminalRecord;
import com.codejianhongxie.po.SdbConnectionInfo;
import com.codejianhongxie.util.DateUtils;
import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;

public class ValidateResultWriterThread implements Runnable {

    private final static Logger logger = LoggerFactory.getLogger(ValidateResultWriterThread.class);

    private BufferedExchanger<Record<BSONObject>> bufferedExchanger;
    private BufferedWriter writer;
    private boolean isStop;

    public ValidateResultWriterThread(BufferedExchanger<Record<BSONObject>> bufferedExchanger, SdbConnectionInfo connectionInfo) throws Exception {
        this.bufferedExchanger = bufferedExchanger;
        String filename = System.getProperty("user.dir") +
                File.separator + connectionInfo.getCollectionSpace()  +
                "." + connectionInfo.getCollection() + "_" + DateUtils.format(new Date(), DateUtils.DEFAULT_FORMAT) + ".dat";
        writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename,true), StandardCharsets.UTF_8));
    }

    @Override
    public void run() {

        while (!isStop) {

            try {
                if (!bufferedExchanger.isEmpty()) {
                    Record<BSONObject> recordData = bufferedExchanger.take();

                    BsonRecordData bsonRecordData = (BsonRecordData<BSONObject>) recordData;
                    @SuppressWarnings("unchecked")
                    List<BSONObject> oidList = bsonRecordData.getData();
                    StringBuilder sb = new StringBuilder();
                    for (BSONObject bson : oidList) {
                        sb.append(bson.get("Oid")).append(",").append(bson.get("flag")).append("\n");
                    }
                    writer.write(sb.toString());
                    writer.flush();
                }
            } catch (InterruptedException e) {
                logger.warn("数据校验结果线程发生中断异常", e);
                break;
            } catch (IOException e) {
                logger.error("数据校验结果线程保存校验结果发生异常", e);
            }
        }

        try {
            if (!bufferedExchanger.isEmpty()) {
                Record<BSONObject> recordData = bufferedExchanger.take();

                BsonRecordData bsonRecordData = (BsonRecordData<BSONObject>) recordData;
                @SuppressWarnings("unchecked")
                List<BSONObject> oidList = bsonRecordData.getData();
                StringBuilder sb = new StringBuilder();
                for (BSONObject bson : oidList) {
                    sb.append(bson.get("Oid")).append(",").append(bson.get("flag")).append("\n");
                }
                writer.write(sb.toString());
                writer.flush();
            }
        } catch (InterruptedException e) {
            logger.warn("数据校验结果线程发生中断异常", e);
        } catch (IOException e) {
            logger.error("数据校验结果线程保存校验结果发生异常", e);
        } finally {
            try {
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void stop() {
        isStop = true;
    }
}
