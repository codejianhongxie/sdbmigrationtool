package com.codejianhongxie.po;

import java.util.List;

/**
 * @author xiejianhong
 * @description
 * @date 2020/6/30 20:00
 */
public class SdbConnectionInfo {

    /**SequoiaDB 数据库连接用户名*/
    private String username;
    /**SequoiaDB 数据库连接密码*/
    private String password;
    /**SequoiaDB 数据库协调节点地址*/
    private List<String> hosts;
    /**SequoiaDB 集合空间名*/
    private String collectionSpace;
    /**SequoiaDB 集合名*/
    private String collection;
    /**SequoiaDB 连接池大小*/
    private int poolSize;
    /**SequoiaDB 缓冲池大小*/
    private int bufferSize;
    /**Lob对象，true表示lob对象*/
    private boolean lobType;

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public List<String> getHosts() {
        return hosts;
    }

    public void setHosts(List<String> hosts) {
        this.hosts = hosts;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    public String getCollectionSpace() {
        return collectionSpace;
    }

    public void setCollectionSpace(String collectionSpace) {
        this.collectionSpace = collectionSpace;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public boolean isLobType() {
        return lobType;
    }

    public void setLobType(boolean lobType) {
        this.lobType = lobType;
    }
}
