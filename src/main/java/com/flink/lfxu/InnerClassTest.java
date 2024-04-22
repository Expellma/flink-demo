package com.flink.lfxu;

public class InnerClassTest {
    private static InnerClassTest instance = new InnerClassTest();

    public class InnerClass {
        public byte f1;
        public  byte f2;

        public InnerClass(byte b1, byte b2) {
            this.f1 = b1;
            this.f2 = b2;
        }

        public InnerClass(byte b1) {
            this(b1, "2".getBytes()[0]);
        }
    }

    public InnerClass findInnerClass(int type) {
        if (type == 1) {
           return new InnerClass("1".getBytes()[0], "2".getBytes()[0]);
        }
       return new InnerClass("1".getBytes()[0]);

    }

    public static InnerClassTest getInstance() {
        return instance;
    }
}
