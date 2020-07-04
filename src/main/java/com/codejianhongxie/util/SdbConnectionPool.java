package com.codejianhongxie.util;

import com.codejianhongxie.po.SdbConnectionInfo;
import com.sequoiadb.base.ConfigOptions;
import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.datasource.ConnectStrategy;
import com.sequoiadb.datasource.DatasourceOptions;
import com.sequoiadb.datasource.SequoiadbDatasource;

/**
 * @author codejianhongxie
 * @description
 * @date 2020/6/26 17:27
 */
public class SdbConnectionPool {

    private SequoiadbDatasource ds;
    private Sequoiadb sdb;
    private static SdbConnectionInfo sdbConnection;

    private SdbConnectionPool() {

        ConfigOptions nwOpt = new ConfigOptions();
        DatasourceOptions dsOpt = new DatasourceOptions();

        /*建立连接超时时间为 500ms */
        nwOpt.setConnectTimeout(500);
        /*建立连接失败后重试时间为 0 ms , 表示不重试 */
        nwOpt.setMaxAutoConnectRetryTime(0);
        /*socket 通信超时时间, 600s*/
        nwOpt.setSocketTimeout(600 * 1000);

        /*最大连接池数量*/
        dsOpt.setMaxCount(sdbConnection.getPoolSize());
        /*每次新增连接数量*/
        dsOpt.setDeltaIncCount(sdbConnection.getPoolSize());
        /*空闲时，连接池保留的连接数量*/
        dsOpt.setMaxIdleCount(sdbConnection.getPoolSize());
        /*连接池中空闲连接存活时间, 0表示不关心连接隔多长时间没有收发消息*/
        dsOpt.setKeepAliveTimeout(0);
        /*每隔60秒将连接池中多于MaxIdleCount限定的空闲连接关闭，并将存活时间过长（连接已停止收发超过keepAliveTimeout时间）的连接关闭*/
        dsOpt.setCheckInterval(600000);
        /*向catalog同步coord地址的周期。单位:毫秒。0表示不同步。*/
        dsOpt.setSyncCoordInterval(0);
        /*连接出池时，检测连接的可用性 */
        dsOpt.setValidateConnection(true);
        /*使用coord地址负载均衡的策略获取连接*/
        dsOpt.setConnectStrategy(ConnectStrategy.BALANCE);

        ds = new SequoiadbDatasource(sdbConnection.getHosts(), sdbConnection.getUsername(), sdbConnection.getPassword(), nwOpt, dsOpt);

    }

    private static class HolderConnectionPool {
        private final static SdbConnectionPool INSTANCE = new SdbConnectionPool();
    }

    public static SdbConnectionPool getInstance(SdbConnectionInfo sdbConnectionInfo) {
        sdbConnection = sdbConnectionInfo;
        return HolderConnectionPool.INSTANCE;
    }

    public synchronized Sequoiadb getConnection() {
        try {
            sdb = ds.getConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sdb;
    }

    public synchronized void close(Sequoiadb sdb) {
        try {
            if (sdb != null) {
                ds.releaseConnection(sdb);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
