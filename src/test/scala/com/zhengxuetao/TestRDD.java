package com.zhengxuetao;

import org.junit.Assert;
import org.junit.Test;

public class TestRDD {
    private com.zhengxuetao.RDDLearning test = new com.zhengxuetao.RDDLearning();

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

    @Test
    public void testFlatMap() {
        test.testFlatMap();
    }
}
