package com.codejianhongxie.util;

import java.text.DecimalFormat;

public class Stringify {

    private final static DecimalFormat df = new DecimalFormat("0.00");
    private final static long KB_IN_BYTES = 1024;
    private final static long MB_IN_BYTES = 1024 * KB_IN_BYTES;
    private final static long GB_IN_BYTES = 1024 * MB_IN_BYTES;
    private final static long TB_IN_BYTES = 1024 * GB_IN_BYTES;

    public static String format(Object number,String suffix){
        return df.format(number) + (suffix==null?"":suffix);
    }

    public static String stringify(long byteNumber,String suffix) {
        if (byteNumber / TB_IN_BYTES > 0) {
            return df.format((double) byteNumber / (double) TB_IN_BYTES) + "TB"+ (suffix==null?"":suffix);
        } else if (byteNumber / GB_IN_BYTES > 0) {
            return df.format((double) byteNumber / (double) GB_IN_BYTES) + "GB"+ (suffix==null?"":suffix);
        } else if (byteNumber / MB_IN_BYTES > 0) {
            return df.format((double) byteNumber / (double) MB_IN_BYTES) + "MB"+ (suffix==null?"":suffix);
        } else if (byteNumber / KB_IN_BYTES > 0) {
            return df.format((double) byteNumber / (double) KB_IN_BYTES) + "KB"+ (suffix==null?"":suffix);
        } else {
            return String.valueOf(byteNumber) + "B"+ (suffix==null?"":suffix);
        }
    }
}
