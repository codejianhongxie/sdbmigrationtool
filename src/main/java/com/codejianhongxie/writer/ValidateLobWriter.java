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
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.List;

/**
 * @author xiejianhong
 * @description
 * @date 2020/7/1 9:36
 */
public class ValidateLobWriter implements Writer {

    private static final Logger logger = LoggerFactory.getLogger(ValidateLobWriter.class);
    private BufferedExchanger<Record<BSONObject>> bufferedExchanger;
    private BufferedExchanger<Record<BSONObject>> failedBufferedExchanger;
    private SdbConnectionInfo sourceConnection;
    private SdbConnectionInfo dstConnection;
    private Sequoiadb sourceSdb;
    private Sequoiadb dstSdb;
    private DBCollection sourceCl;
    private DBCollection dstCl;
    private BandwidthLimiter bandwidthLimiter;

    public ValidateLobWriter(BufferedExchanger<Record<BSONObject>> bufferedExchanger, BufferedExchanger<Record<BSONObject>> failedBufferedExchanger, SdbConnectionInfo sourceConnection, SdbConnectionInfo dstConnection, int maxRate) {
        this.bufferedExchanger = bufferedExchanger;
        this.failedBufferedExchanger = failedBufferedExchanger;
        this.sourceConnection = sourceConnection;
        ConfigOptions configOptions = new ConfigOptions();
        configOptions.setSocketTimeout(300 * 1000);
        this.sourceSdb = new Sequoiadb(sourceConnection.getHosts(), sourceConnection.getUsername(), sourceConnection.getPassword(), configOptions);
        this.sourceCl = sourceSdb.getCollectionSpace(sourceConnection.getCollectionSpace())
                .getCollection(sourceConnection.getCollection());
        this.dstConnection = dstConnection;
        this.dstSdb = SdbConnectionPool.getInstance(dstConnection).getConnection();
        this.dstCl = dstSdb.getCollectionSpace(dstConnection.getCollectionSpace())
                .getCollection(dstConnection.getCollection());
        this.bandwidthLimiter = new BandwidthLimiter(maxRate);
    }


    @Override
    public void process(BsonRecordData recordData) throws Exception {

        @SuppressWarnings("unchecked")
        List<BSONObject> oidList = recordData.getData();
        BsonRecordData<BSONObject> resultRecord = new BsonRecordData<>();
        for (BSONObject bson : oidList) {
            ObjectId oid = (ObjectId) bson.get("Oid");
            long sourceLobSize = (long)bson.get("Size");
            DBLob dstLob = null;
            DBLob sourceLob = null;
            try {
                dstLob = dstCl.openLob(oid);

                long dstLobSize = dstLob.getSize();

                if (sourceLobSize != dstLobSize) {
                    BSONObject record = new BasicBSONObject();
                    record.put("Oid", oid);
                    record.put("flag", -1);
                    resultRecord.add(record);
                    Metric.increaseFailedCount(1);
                } else {
                    sourceLob = sourceCl.openLob(oid);
                    String sourceLobMd5 = getLobMd5(sourceLob);
                    String dstLobMd5 = getLobMd5(dstLob);

                    if (!sourceLobMd5.equals(dstLobMd5)) {
                        BSONObject record = new BasicBSONObject();
                        record.put("Oid", oid);
                        record.put("flag", -2);
                        resultRecord.add(record);
                        Metric.increaseFailedCount(1);
                    } else {
                        Metric.increaseExistedCount(1);
                    }
                }
            } catch (Exception e) {

                if (e instanceof BaseException) {
                    BaseException baseException = (BaseException)e;
                    if (baseException.getErrorCode() == -4 ) {
                        logger.error("Lob对象{}在目标端集合{}中不存在", oid.toString(), dstCl.getFullName());
                        BSONObject record = new BasicBSONObject();
                        record.put("Oid", oid);
                        record.put("flag", -4);
                        resultRecord.add(record);
                        Metric.increaseNotExistedCount(1);
                    } else {
                        logger.error("数据校验线程{}发生异常, 退出程序", Thread.currentThread().getName(), e);
                        System.exit(-1);
                    }
                } else {
                    logger.error("数据校验线程{}发生异常, 退出程序", Thread.currentThread().getName(), e);
                    System.exit(-1);
                }
            } finally {
                Metric.increaseWriteCount(1);
                if (sourceLob != null) {
                    sourceLob.close();
                }
                if (dstLob != null) {
                    dstLob.close();
                }
            }
        }

        if (resultRecord.getLength() > 0) {
            failedBufferedExchanger.put(resultRecord);
        }
    }

    @Override
    public void close() {

        if (sourceSdb != null) {
            sourceSdb.close();
        }

        if (dstSdb != null) {
            SdbConnectionPool.getInstance(dstConnection).close(dstSdb);
        }
    }

    private String getLobMd5(DBLob lob) throws Exception {

        BigInteger md5Value = null;
        try {
            byte[] buffer = new byte[512 * 1024];
            int len = 0;
            MessageDigest md = MessageDigest.getInstance("MD5");
            while ((len = lob.read(buffer)) != -1) {
                md.update(buffer, 0, len);
                Metric.increaseTransferSpeed(len);
                bandwidthLimiter.limitNextBytes(len);
            }
            byte[] b = md.digest();
            md5Value = new BigInteger(1, b);
        } catch (Exception e) {
            logger.error("获取 Lob 对象的 md5 值失败", e);
            throw e;
        }
        return md5Value.toString(16);
    }
}
