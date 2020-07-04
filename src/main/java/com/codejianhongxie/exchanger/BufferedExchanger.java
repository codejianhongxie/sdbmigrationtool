package com.codejianhongxie.exchanger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author xiejianhong
 * @description
 * @date 2020/6/26 16:40
 */
public class BufferedExchanger<E> {

    private BlockingQueue<E> queue;

    public BufferedExchanger(){
        this(10);
    }

    public BufferedExchanger(int capacity){
        queue = new LinkedBlockingQueue<E>(capacity);
    }

    public int size(){
        return queue.size();
    }

    public boolean isEmpty(){
        return queue.isEmpty();
    }

    public void put(E e) throws InterruptedException{
        queue.put(e);
    }

    public E take() throws InterruptedException {
        return queue.take();
    }
}
