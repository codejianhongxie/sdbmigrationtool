package com.codejianhongxie.executor;

import com.codejianhongxie.exchanger.BufferedExchanger;
import com.codejianhongxie.exchanger.Record;
import com.codejianhongxie.po.SdbConnectionInfo;
import com.codejianhongxie.reader.Reader;
import com.codejianhongxie.reader.ValidateLobReader;
import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author xiejianhong
 * @description
 * @date 2020/6/26 16:50
 */
public class ValidateReaderThread implements BaseThread {

    private final static Logger logger = LoggerFactory.getLogger(ValidateReaderThread.class);

    private Reader reader;

    public ValidateReaderThread(BufferedExchanger<Record<BSONObject>> bufferedExchanger,
                                SdbConnectionInfo connectionInfo) {
        reader = new ValidateLobReader(bufferedExchanger, connectionInfo);
    }

    @Override
    public void stop() {
        reader.stop();
    }

    @Override
    public void run() {

        try {
            reader.process();
        } catch (Exception e) {
            logger.error("迁移读取操作的线程发生异常", e);
            Thread.currentThread().interrupt();
        }
    }
}
