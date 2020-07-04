package com.codejianhongxie.exchanger;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiejianhong
 * @description
 * @date 2020/6/26 16:43
 */
public class BsonRecordData<E> implements Record {

    private List<E> list = new ArrayList<E>();
    private int length = 0;

    public void add(E e){
        this.list.add(e);
        this.length++;
    }

    public int getLength(){
        return this.length;
    }

    public List<E> getData(){
        return this.list;
    }
}
