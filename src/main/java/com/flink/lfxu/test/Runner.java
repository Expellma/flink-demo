package com.flink.lfxu.test;

import com.flink.lfxu.InnerClassTest;

public class Runner {
    public static void main(String[] args){
        int type1 =1;
        int type2 =2;
        InnerClassTest.InnerClass innerClass = InnerClassTest.getInstance().findInnerClass(type1);
        InnerClassTest.InnerClass innerClass2 = InnerClassTest.getInstance().findInnerClass(type2);
        System.out.println(innerClass2.f1);
    }
}
