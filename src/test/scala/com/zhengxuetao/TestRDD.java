package com.zhengxuetao;

import org.junit.Assert;
import org.junit.Test;

public class TestRDD {
    com.zhengxuetao.RDD test = new com.zhengxuetao.RDD();

    @Test
    public void testFilter() {
        Assert.assertEquals(test.testFilter(), 3);
    }

    @Test
    public void testMap() {
        String[][] arr = test.testMap();
        for (String[] arr1 : arr) {
            for (String s : arr1) {
                System.out.println(s);
            }
        }
    }
}
