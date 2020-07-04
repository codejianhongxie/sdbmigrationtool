package com.codejianhongxie.writer;

import com.codejianhongxie.exchanger.BsonRecordData;
import com.codejianhongxie.exchanger.BufferedExchanger;
import com.codejianhongxie.exchanger.Record;
import com.codejianhongxie.po.SdbConnectionInfo;
import com.codejianhongxie.util.BandwidthLimiter;
import com.codejianhongxie.util.Metric;
import com.codejianhongxie.util.SdbConnectionPool;
import com.sequoiadb.base.ConfigOptions;
import com.sequoiadb.base.DBCollection;
import com.sequoiadb.base.DBLob;
import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.exception.BaseException;
import org.bson.BSONObject;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author xiejianhong
 * @description
 * @date 2020/6/30 19:30
 */
public class MigrationLobWriter implements Writer {

    private static final Logger logger = LoggerFactory.getLogger(MigrationLobWriter.class);

    private final static int SDB_LOB_FILE_IS_EXISTS = -5;
    private final static int SDB_LOB_IS_NOT_AVAILABLE = -269;

    private BufferedExchanger<Record<BSONObject>> bufferedExchanger;
    private SdbConnectionInfo sourceConnection;
    private SdbConnectionInfo destConnection;
    private Sequoiadb sourceSdb;
    private Sequoiadb destSdb;
    private DBCollection sourceCl;
    private DBCollection destCl;
    private BandwidthLimiter bandwidthLimiter;

    public MigrationLobWriter(BufferedExchanger<Record<BSONObject>> bufferedExchanger, SdbConnectionInfo sourceConnection, SdbConnectionInfo destConnection) {
        this(bufferedExchanger, sourceConnection, destConnection, 0);
    }

    public MigrationLobWriter(BufferedExchanger<Record<BSONObject>> bufferedExchanger, SdbConnectionInfo sourceConnection, SdbConnectionInfo dstConnection, int maxRate) {
        this.bufferedExchanger = bufferedExchanger;
        this.sourceConnection = sourceConnection;
        ConfigOptions configOptions = new ConfigOptions();
        configOptions.setSocketTimeout(300 * 1000);
        this.sourceSdb = new Sequoiadb(sourceConnection.getHosts(), sourceConnection.getUsername(), sourceConnection.getPassword(), configOptions);
        this.sourceCl = sourceSdb.getCollectionSpace(sourceConnection.getCollectionSpace())
                .getCollection(sourceConnection.getCollection());
        this.destConnection = dstConnection;
        this.destSdb = SdbConnectionPool.getInstance(dstConnection).getConnection();
        this.destCl = destSdb.getCollectionSpace(dstConnection.getCollectionSpace())
                .getCollection(dstConnection.getCollection());
        this.bandwidthLimiter = new BandwidthLimiter(maxRate);
    }

    @Override
    public void process(BsonRecordData recordData) throws Exception {

        @SuppressWarnings("unchecked")
        List<BSONObject> oidList = recordData.getData();
        for (BSONObject bson : oidList) {
            ObjectId oid = (ObjectId) bson.get("Oid");
            DBLob sourceLob = null;
            DBLob dstLob = null;
            try {
                sourceLob = sourceCl.openLob(oid);
                dstLob = destCl.createLob(oid);

                byte[] buffer = new byte[512 * 1024];
                int length;
                while ((length = sourceLob.read(buffer)) != -1) {
                    dstLob.write(buffer, 0, length);
                    bandwidthLimiter.limitNextBytes(length);
                    Metric.increaseTransferSpeed(length);
                }
                Metric.increaseWriteCount(1);
            } catch (BaseException e) {
                if (e.getErrorCode() != SDB_LOB_FILE_IS_EXISTS) {
                    Metric.increaseFailedCount(1);
                    if (e.getErrorCode() == SDB_LOB_IS_NOT_AVAILABLE) {
                        logger.error("写线程{}写 Lob 对象失败", Thread.currentThread().getName(), e);
                        throw new Exception(e);
                    } else {
                        logger.warn("Lob 对象 [{}] is not available", oid.toString());
                    }
                } else {
                    Metric.increaseExistedCount(1);
                }
            } finally {
                if (sourceLob != null) {
                    sourceLob.close();
                }
                if (dstLob != null) {
                    dstLob.close();
                }
            }
        }
    }

    @Override
    public void close() {

        if (sourceSdb != null) {
            sourceSdb.close();
        }

        if (destSdb != null) {
            SdbConnectionPool.getInstance(destConnection).close(destSdb);
        }
    }
}
