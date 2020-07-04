package com.codejianhongxie.reader;

/**
 * @author xiejianhong
 * @description
 * @date 2020/6/26 16:50
 */
public interface Reader {

    /**
     * 读操作处理
     * @throws Exception
     */
    void process() throws Exception;

    /**
     * 停止读取
     */
    void stop();
}
