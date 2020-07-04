package com.codejianhongxie.reader;

import com.codejianhongxie.exchanger.BsonRecordData;
import com.codejianhongxie.exchanger.BufferedExchanger;
import com.codejianhongxie.exchanger.Record;
import com.codejianhongxie.exchanger.TerminalRecord;
import com.codejianhongxie.po.SdbConnectionInfo;
import com.codejianhongxie.util.Metric;
import com.sequoiadb.base.ConfigOptions;
import com.sequoiadb.base.DBCollection;
import com.sequoiadb.base.DBCursor;
import com.sequoiadb.base.Sequoiadb;
import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author xiejianhong
 * @description
 * @date 2020/6/26 16:53
 */
public class MigrationReader implements Reader {

    private final static Logger logger = LoggerFactory.getLogger(MigrationReader.class);

    private BufferedExchanger<Record<BSONObject>> bufferedExchanger;
    private SdbConnectionInfo sourceConnection;
    private boolean isStop = false;

    public MigrationReader(BufferedExchanger<Record<BSONObject>> bufferedExchanger, SdbConnectionInfo sourceConnection) {
        this.bufferedExchanger = bufferedExchanger;
        this.sourceConnection = sourceConnection;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void process() throws Exception {


        ConfigOptions configOptions = new ConfigOptions();
        configOptions.setSocketTimeout(300 * 1000);
        List<String> hosts = sourceConnection.getHosts();
        String user = sourceConnection.getUsername();
        String password = sourceConnection.getPassword();
        int bufferSize = sourceConnection.getBufferSize();
        String csName = sourceConnection.getCollectionSpace();
        String clName = sourceConnection.getCollection();


        Sequoiadb sdb = null;
        DBCursor cursor = null;
        try {
            //logger.info("数据读取线程获取 SequoiaDB 连接");
            sdb = new Sequoiadb(hosts, user, password, configOptions);
            DBCollection cl = sdb.getCollectionSpace(csName).getCollection(clName);

            if (sourceConnection.isLobType()) {
                //logger.info("数据读取线程获取集合{}的 Lob 对象", cl.getFullName());
                cursor = cl.listLobs();

            } else {
                //logger.info("数据读取线程获取集合{}的 json 数据", cl.getFullName());
                cursor = cl.query();
            }

            BsonRecordData<BSONObject> bsonRecordData = new BsonRecordData<>();
            while (cursor.hasNext()) {
                if (!isStop) {
                    BSONObject bson = cursor.getNext();
                    bsonRecordData.add(bson);
                    Metric.increaseReadCount(1);
                    if (bsonRecordData.getLength() >= bufferSize) {
                        bufferedExchanger.put(bsonRecordData);
                        bsonRecordData = new BsonRecordData<BSONObject>();
                    }
                } else {
                    BSONObject bson = cursor.getNext();
                    bsonRecordData.add(bson);
                    Metric.increaseReadCount(1);
                    logger.info("停止数据读取,当前缓冲区大小为:" + bufferedExchanger.size());
                    break;
                }
            }
            if (bsonRecordData.getLength() >= 1) {
                bufferedExchanger.put(bsonRecordData);
            }
            logger.info("数据读取线程读取数据完毕, 读取的总数为{}", Metric.getReadCount());
            bufferedExchanger.put(new TerminalRecord());
        } catch (Exception e) {
            logger.error("数据读取发生异常", e);
        } finally {

            if (cursor != null) {
                cursor.close();
            }
            if (sdb != null) {
                sdb.close();
            }
        }
    }

    @Override
    public void stop() {
        isStop = true;
    }
}
