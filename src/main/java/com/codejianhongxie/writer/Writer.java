package com.codejianhongxie.writer;

import com.codejianhongxie.exchanger.BsonRecordData;

/**
 * @author xiejianhong
 * @description
 * @date 2020/6/30 19:30
 */
public interface Writer<T> {

    /**
     * 写操作处理
     * @throws Exception
     */
    void process(BsonRecordData<T> recordData) throws Exception;

    /**
     * 关闭写操作
     */
    void close();
}
