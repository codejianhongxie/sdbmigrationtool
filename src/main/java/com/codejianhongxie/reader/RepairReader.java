package com.codejianhongxie.reader;

import com.codejianhongxie.exchanger.BsonRecordData;
import com.codejianhongxie.exchanger.BufferedExchanger;
import com.codejianhongxie.exchanger.Record;
import com.codejianhongxie.exchanger.TerminalRecord;
import com.codejianhongxie.util.Constants;
import com.codejianhongxie.util.Metric;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * @author xiejianhong
 * @description
 * @date 2020/6/26 16:53
 */
public class RepairReader implements Reader {

    private static final Logger logger = LoggerFactory.getLogger(RepairReader.class);
    private BufferedExchanger<Record<BSONObject>> bufferedExchanger;
    private String fileName;
    private int bufferSize;
    private boolean isStop = false;

    public RepairReader(BufferedExchanger<Record<BSONObject>> bufferedExchanger, String fileName, int bufferSize) {
        this.bufferedExchanger = bufferedExchanger;
        this.fileName = fileName;
        this.bufferSize = bufferSize;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void process() throws Exception {

        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            BsonRecordData<BSONObject> recordData = new BsonRecordData<>();
            String line;
            while ((line = reader.readLine()) != null) {
                if (!isStop) {
                    if (line.length() > 0 && line.contains(Constants.POOL_CONNECTION_SEPARATOR)) {
                        String[] strs = line.split(Constants.POOL_CONNECTION_SEPARATOR);
                        BSONObject resultBson = new BasicBSONObject();
                        resultBson.put("Oid", new ObjectId(strs[0]));
                        resultBson.put("Flag", Integer.valueOf(strs[1]));
                        recordData.add(resultBson);
                        if (recordData.getLength() >= bufferSize) {
                            bufferedExchanger.put(recordData);
                            recordData = new BsonRecordData<>();
                        }
                        Metric.increaseReadCount(1);
                    }
                } else {
                    if (line.length() > 0 && line.contains(Constants.POOL_CONNECTION_SEPARATOR)) {
                        String[] strs = line.split(Constants.POOL_CONNECTION_SEPARATOR);
                        BSONObject resultBson = new BasicBSONObject();
                        resultBson.put("Oid", new ObjectId(strs[0]));
                        resultBson.put("Flag", Integer.valueOf(strs[1]));
                        recordData.add(resultBson);
                        if (recordData.getLength() >= bufferSize) {
                            bufferedExchanger.put(recordData);
                            recordData = new BsonRecordData<>();
                        }
                        Metric.increaseReadCount(1);
                        logger.info("停止影像元数据读取,当前缓冲区大小为:" + bufferedExchanger.size());
                        break;
                    }
                }
            }
            if (recordData.getLength() >= 1) {
                bufferedExchanger.put(recordData);
            }

            bufferedExchanger.put(new TerminalRecord());
        } catch (Exception e) {
            logger.error("读线程{}读取修复对象Oid文件发生异常", Thread.currentThread().getName(), e);
            throw new Exception(e);
        }

    }

    @Override
    public void stop() {
        isStop = true;
    }
}
