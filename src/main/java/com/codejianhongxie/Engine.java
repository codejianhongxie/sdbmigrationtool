package com.codejianhongxie;

import com.codejianhongxie.exchanger.BufferedExchanger;
import com.codejianhongxie.exchanger.Record;
import com.codejianhongxie.executor.*;
import com.codejianhongxie.po.SdbConnectionInfo;
import com.codejianhongxie.util.*;
import com.sequoiadb.base.ConfigOptions;
import com.sequoiadb.base.ReplicaGroup;
import com.sequoiadb.base.Sequoiadb;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.bson.BSONObject;
import org.bson.types.BasicBSONList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author xiejianhong
 * @description 数据迁移工具入口类
 * @date 2020/6/26 10:08
 */
public class Engine {


    private final static Logger logger = LoggerFactory.getLogger(Engine.class);

    public static void main(String[] args) {

        MigrationOption migrationOption = new MigrationOption();

        if (args.length <= 0) {
            migrationOption.printUsage();
            System.exit(ExitCode.NORMAL_EXIT.code);
        }

        CommandLineParser parser = new DefaultParser();
        Options options = migrationOption.getMigrationOptions();
        try {
            CommandLine line = parser.parse(options, args);
            migrationOption.parseOptions(line);


            SdbConnectionInfo sourceConnection = new SdbConnectionInfo();
            SdbConnectionInfo dstConnection = new SdbConnectionInfo();

            ArrayList<String> coordAddr = new ArrayList<String>();
            String[] urls = migrationOption.getHosts().split(",");
            Collections.addAll(coordAddr, urls);
            sourceConnection.setHosts(coordAddr);
            sourceConnection.setUsername(migrationOption.getUser());
            String clFullName = migrationOption.getCollection();
            int clIndex = clFullName.indexOf(Constants.COLLECTION_SEPARATOR);
            sourceConnection.setCollectionSpace(clFullName.substring(0, clIndex));
            sourceConnection.setCollection(clFullName.substring(clIndex + 1));
            sourceConnection.setBufferSize(migrationOption.getInsertNum());
            sourceConnection.setPoolSize(migrationOption.getJobs() * 2);

            ArrayList<String> dstCoordAddr = new ArrayList<String>();
            String[] dstUrls = migrationOption.getDstHosts().split(",");
            Collections.addAll(dstCoordAddr, dstUrls);
            dstConnection.setHosts(dstCoordAddr);
            dstConnection.setUsername(migrationOption.getDstUser());
            String dstClFullName = migrationOption.getDstCollection();
            int dstClIndex = dstClFullName.indexOf(Constants.COLLECTION_SEPARATOR);
            dstConnection.setCollectionSpace(dstClFullName.substring(0, dstClIndex));
            dstConnection.setCollection(dstClFullName.substring(dstClIndex + 1));
            dstConnection.setBufferSize(migrationOption.getInsertNum());
            dstConnection.setPoolSize(migrationOption.getJobs() * 2);

            if ("sm2".equalsIgnoreCase(migrationOption.getEncryptType())) {
                sourceConnection.setPassword(PasswordUtil.passwordDecrypt(migrationOption.getPassword()));
                dstConnection.setPassword(PasswordUtil.passwordDecrypt(migrationOption.getDstPassword()));
            } else {
                sourceConnection.setPassword(migrationOption.getPassword());
                dstConnection.setPassword(migrationOption.getDstPassword());
            }

            if ("lob".equalsIgnoreCase(migrationOption.getType())) {
                sourceConnection.setLobType(true);
                dstConnection.setLobType(true);
            } else {
                sourceConnection.setLobType(false);
                sourceConnection.setLobType(false);
            }

            if (migrationOption.isCoord()) {
                List<String> coords = getCoordAddr(dstConnection);
                dstConnection.setHosts(coords);
            }

            if (migrationOption.isMigration()) {
                migration(sourceConnection, dstConnection, migrationOption);
            } else if (migrationOption.isValidate()) {
                validateLob(sourceConnection, dstConnection, migrationOption);
            } else if (migrationOption.isRepair()) {
                repairLob(sourceConnection, dstConnection, migrationOption);
            }
        } catch (Exception e) {
            logger.error("数据迁移执行失败", e);
            System.exit(ExitCode.ERROR_EXIT.code);
        }
    }

    /**
     * 获取 SequoiaDB 数据库的协调节点地址列表
     * @param sdbConnection
     * @return
     */
    private static List<String> getCoordAddr(SdbConnectionInfo sdbConnection) {

        List<String> coordAddr = new ArrayList<>();
        Sequoiadb sdb = null;
        ConfigOptions configOptions = new ConfigOptions();
        configOptions.setSocketTimeout(300 * 1000);
        try {
            sdb = new Sequoiadb(sdbConnection.getHosts(), sdbConnection.getUsername(), sdbConnection.getPassword(), configOptions);
            ReplicaGroup coordGroup = sdb.getReplicaGroup("SYSCoord");
            BSONObject details = coordGroup.getDetail();
            BasicBSONList groups = (BasicBSONList) details.get("Group");

            for(int i = 0;  i < groups.size(); i++) {
                BSONObject record = (BSONObject)groups.get(i);
                BasicBSONList servicesList = (BasicBSONList) record.get("Service");
                String hostName = (String)record.get("HostName");
                for(Object obj : servicesList) {
                    BSONObject services = (BSONObject)obj;
                    int type = (int)services.get("Type");
                    if (type == 0) {
                        String serviceName = (String)services.get("Name");
                        coordAddr.add(hostName + ":" + serviceName);
                    }
                }
            }
        } finally {
            if (sdb != null) {
                sdb.close();
            }
        }
        return coordAddr;
    }

    /**
     * Lob 对象数据校验
     */
    private static void validateLob(SdbConnectionInfo sourceConnection, SdbConnectionInfo dstConnection,
                                    MigrationOption migrationOption) throws Exception {

        String startTime= DateUtils.format(Metric.getTimestamp());
        String operation = migrationOption.getOperation();
        String type = migrationOption.getType();
        boolean isLob = "lob".equalsIgnoreCase(type);
        int jobs = migrationOption.getJobs();
        int perThreadMaxRate = migrationOption.getMaxRate() / jobs;
        int reportInterval = migrationOption.getReportInterval();
        ThreadExecutorFactory executorFactory = new ThreadExecutorFactory();
        BufferedExchanger<Record<BSONObject>> bufferedExchanger = new BufferedExchanger<>(jobs);
        BufferedExchanger<Record<BSONObject>> validateResultExchanger = new BufferedExchanger<>(jobs);
        ValidateReaderThread readerThread = new ValidateReaderThread(bufferedExchanger, sourceConnection);


        ExecutorService readerExecutor = executorFactory.getExecutor("reader", 1, 1, 1);
        readerExecutor.submit(readerThread);

        StopHandler handler = new StopHandler(readerThread);
        handler.registerSignal("TERM");
        handler.registerSignal("INT");

        ValidateResultWriterThread validateResultWriterThread = new ValidateResultWriterThread(validateResultExchanger, sourceConnection);
        ExecutorService validateResultExecutor = executorFactory.getExecutor("result", 1, 1, 1);
        validateResultExecutor.submit(validateResultWriterThread);

        ExecutorService writerExecutor = executorFactory.getExecutor("writer", jobs * 2, jobs, 1);
        for (int i = 0; i < jobs; i++) {
            ValidateWriterThread writerThread =
                    new ValidateWriterThread(bufferedExchanger, validateResultExchanger, sourceConnection, dstConnection, perThreadMaxRate);
            writerExecutor.submit(writerThread);
        }

        ScheduledExecutorService monitorExecutor = null;
        if (reportInterval > 0) {
            MetricThread metricTask = new MetricThread(operation, isLob);
            monitorExecutor = Executors.newSingleThreadScheduledExecutor();
            monitorExecutor.scheduleAtFixedRate(metricTask, 2, reportInterval, TimeUnit.SECONDS);
        }

        writerExecutor.shutdown();
        readerExecutor.shutdown();
        validateResultExecutor.shutdown();

        try {
            while (!writerExecutor.awaitTermination(1, TimeUnit.SECONDS)) ;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (reportInterval > 0) {
            monitorExecutor.shutdown();
        }
        validateResultWriterThread.stop();

        long timeInterval = System.currentTimeMillis() - Metric.getTimestamp();
        long sec = timeInterval <= 1000 ? 1 : timeInterval / 1000;
        String metric = String.format(
                "%s:%s" +
                        "%s:%s" +
                        "%s:%s" +
                        "%s:%s" +
                        "%s:%s" +
                        "%s:%s" +
                        "%s:%s" ,
                "begin time",startTime,
                ", cost time",Stringify.format(sec,"s"),
                ", total read",Metric.getReadCount(),
                ", total validate",Metric.getWriteCount(),
                ", existed",Metric.getExistedCount(),
                ", not existed",Metric.getNotExistedCount(),
                ", total failed",Metric.getFailedCount());
        logger.info(metric);
    }

    /**
     * Lob 对象修复操作
     *
     * @param migrationOption 参数选项
     */
    private static void repairLob(SdbConnectionInfo sourceConnection, SdbConnectionInfo dstConnection,
                                  MigrationOption migrationOption) throws Exception {

        String startTime= DateUtils.format(Metric.getTimestamp());
        String fileName = migrationOption.getFileName();
        String operation = migrationOption.getOperation();
        String type = migrationOption.getType();
        boolean isLob = "lob".equalsIgnoreCase(type);
        int jobs = migrationOption.getJobs();
        int perThreadMaxRate = migrationOption.getMaxRate() / jobs;
        int reportInterval = migrationOption.getReportInterval();
        ThreadExecutorFactory executorFactory = new ThreadExecutorFactory();
        BufferedExchanger<Record<BSONObject>> bufferedExchanger = new BufferedExchanger<>(jobs);
        RepairReaderThread readerThread = new RepairReaderThread(bufferedExchanger, fileName, sourceConnection.getBufferSize());


        ExecutorService readerExecutor = executorFactory.getExecutor("reader", 1, 1, 1);
        readerExecutor.submit(readerThread);

        StopHandler handler = new StopHandler(readerThread);
        handler.registerSignal("TERM");
        handler.registerSignal("INT");


        ExecutorService writerExecutor = executorFactory.getExecutor("writer", jobs * 2, jobs, 1);
        for (int i = 0; i < jobs; i++) {
            RepairWriterThread writerThread =
                    new RepairWriterThread(bufferedExchanger, sourceConnection, dstConnection, perThreadMaxRate);
            writerExecutor.submit(writerThread);
        }

        ScheduledExecutorService monitorExecutor = null;
        if (reportInterval > 0) {
            MetricThread metricTask = new MetricThread(operation, isLob);
            monitorExecutor = Executors.newSingleThreadScheduledExecutor();
            monitorExecutor.scheduleAtFixedRate(metricTask, 2, reportInterval, TimeUnit.SECONDS);
        }

        writerExecutor.shutdown();
        readerExecutor.shutdown();

        try {
            while (!writerExecutor.awaitTermination(1, TimeUnit.SECONDS)) ;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (reportInterval > 0) {
            monitorExecutor.shutdown();
        }
        long timeInterval = System.currentTimeMillis() - Metric.getTimestamp();
        long sec = timeInterval <= 1000 ? 1 : timeInterval / 1000;
        String metric = String.format(
                "%s:%s" +
                        "%s:%s" +
                        "%s:%s" +
                        "%s:%s" +
                        "%s:%s" +
                        "%s:%s" +
                        "%s:%s" ,
                "begin time",startTime,
                ", cost time",Stringify.format(sec,"s"),
                ", total read",Metric.getReadCount(),
                ", total write",Metric.getWriteCount(),
                ", total size",Stringify.stringify(Metric.getTransferSpeed(),null),
                ", existed",Metric.getExistedCount(),
                ", total failed",Metric.getFailedCount());
        logger.info(metric);
    }

    /**
     * 结构化、半结构化数据迁移操作
     */
    private static void migration(SdbConnectionInfo sourceConnection, SdbConnectionInfo dstConnection,
                                  MigrationOption migrationOption) {

        String startTime= DateUtils.format(Metric.getTimestamp());
        String operation = migrationOption.getOperation();
        String type = migrationOption.getType();
        boolean isLob = "lob".equalsIgnoreCase(type);
        int jobs = migrationOption.getJobs();
        int perThreadMaxRate = migrationOption.getMaxRate() / jobs;
        int reportInterval = migrationOption.getReportInterval();
        ThreadExecutorFactory executorFactory = new ThreadExecutorFactory();
        BufferedExchanger<Record<BSONObject>> bufferedExchanger = new BufferedExchanger<Record<BSONObject>>(jobs);
        MigrationReaderThread readerThread = new MigrationReaderThread(bufferedExchanger, sourceConnection);


        ExecutorService readerExecutor = executorFactory.getExecutor("reader", 1, 1, 1);
        readerExecutor.submit(readerThread);

        StopHandler handler = new StopHandler(readerThread);
        handler.registerSignal("TERM");
        handler.registerSignal("INT");


        ExecutorService writerExecutor = executorFactory.getExecutor("writer", jobs * 2, jobs, 1);
        for (int i = 0; i < jobs; i++) {
            MigrationWriterThread writerThread = new MigrationWriterThread(bufferedExchanger, sourceConnection, dstConnection, isLob, perThreadMaxRate);
            writerExecutor.submit(writerThread);
        }

        ScheduledExecutorService monitorExecutor = null;
        if (reportInterval > 0) {

            MetricThread metricTask = new MetricThread(operation, isLob);
            monitorExecutor = Executors.newSingleThreadScheduledExecutor();
            monitorExecutor.scheduleAtFixedRate(metricTask, 2, reportInterval, TimeUnit.SECONDS);
        }

        writerExecutor.shutdown();
        readerExecutor.shutdown();

        try {
            while (!writerExecutor.awaitTermination(1, TimeUnit.SECONDS)) ;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (reportInterval > 0) {
            monitorExecutor.shutdown();
        }

        long timeInterval = System.currentTimeMillis() - Metric.getTimestamp();
        long sec = timeInterval <= 1000 ? 1 : timeInterval / 1000;

        if (isLob) {
            String metric = String.format(
                    "%s:%s" +
                            "%s:%s" +
                            "%s:%s" +
                            "%s:%s" +
                            "%s:%s" +
                            "%s:%s" +
                            "%s:%s" ,
                    "begin time",startTime,
                    ", cost time",Stringify.format(sec,"s"),
                    ", total read",Metric.getReadCount(),
                    ", total write",Metric.getWriteCount(),
                    ", total size",Stringify.stringify(Metric.getTransferSpeed(),null),
                    ", existed",Metric.getExistedCount(),
                    ", total failed",Metric.getFailedCount());
            logger.info(metric);
        } else {
            String metric = String.format(
                    "%s:%s" +
                            "%s:%s" +
                            "%s:%s" +
                            "%s:%s" +
                            "%s:%s",
                    "begin time", startTime,
                    ", cost time", Stringify.format(sec, "s"),
                    ", total read", Metric.getReadCount(),
                    ", total write", Metric.getWriteCount(),
                    ", total failed", Metric.getFailedCount());
            logger.info(metric);
        }
    }

    enum ExitCode {

        /**
         * 发生错误退出
         */
        ERROR_EXIT(-1),
        /**
         * 正常退出
         */
        NORMAL_EXIT(0);

        int code;

        ExitCode(int code) {
            this.code = code;
        }
    }

}
