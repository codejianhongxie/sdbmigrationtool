package com.codejianhongxie.writer;

import com.codejianhongxie.exchanger.BsonRecordData;
import com.codejianhongxie.exchanger.BufferedExchanger;
import com.codejianhongxie.exchanger.Record;
import com.codejianhongxie.po.SdbConnectionInfo;
import com.codejianhongxie.util.Metric;
import com.codejianhongxie.util.SdbConnectionPool;
import com.sequoiadb.base.DBCollection;
import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.exception.BaseException;
import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.sequoiadb.base.DBCollection.FLG_INSERT_CONTONDUP;

/**
 * @author xiejianhong
 * @description
 * @date 2020/6/30 19:30
 */
public class MigrationDataWriter implements Writer {

    private static final Logger logger = LoggerFactory.getLogger(MigrationDataWriter.class);
    private static final int SDB_NETWORK = -15;
    private static final int SDB_NET_CANNOT_CONNECT = -79;
    private static final int SDB_NOT_CONNECTED = -64;

    private BufferedExchanger<Record<BSONObject>> bufferedExchanger;
    private SdbConnectionInfo dstConnection;
    private Sequoiadb dstSdb;
    private DBCollection dstCl;

    public MigrationDataWriter(BufferedExchanger<Record<BSONObject>> bufferedExchanger, SdbConnectionInfo dstConnection) {
        this.bufferedExchanger = bufferedExchanger;
        this.dstConnection = dstConnection;
        this.dstSdb = SdbConnectionPool.getInstance(dstConnection).getConnection();
        this.dstCl = dstSdb.getCollectionSpace(dstConnection.getCollectionSpace())
                .getCollection(dstConnection.getCollection());
    }

    @Override
    public void process(BsonRecordData recordData) throws Exception {


        int retryTime = 3;
        @SuppressWarnings("unchecked")
        List<BSONObject> recordList = recordData.getData();

        while(retryTime > 0) {
            try {
                dstCl.insertRecords(recordList, FLG_INSERT_CONTONDUP);
                Metric.increaseWriteCount(recordData.getLength());
                break;
            } catch (BaseException e) {

                if (e.getErrorCode() == SDB_NETWORK || e.getErrorCode() == SDB_NET_CANNOT_CONNECT || e.getErrorCode() == SDB_NOT_CONNECTED) {
                    logger.error("retry");
                    SdbConnectionPool.getInstance(dstConnection).close(dstSdb);
                    dstSdb = SdbConnectionPool.getInstance(dstConnection).getConnection();
                    dstCl = dstSdb.getCollectionSpace(dstConnection.getCollectionSpace())
                            .getCollection(dstConnection.getCollection());
                    retryTime--;
                } else {
                    Metric.increaseFailedCount(recordData.getLength());
                    logger.error("写线程{}写数据发生异常", Thread.currentThread().getName());
                    throw new Exception(e);
                }
            }
        }

    }

    @Override
    public void close() {

        if (dstSdb != null) {
            SdbConnectionPool.getInstance(dstConnection).close(dstSdb);
        }
    }
}
