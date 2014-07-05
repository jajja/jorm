package com.jajja.jorm.patch;

import java.util.HashMap;

import org.postgresql.util.PGmoney;
import org.postgresql.util.PGobject;

import com.jajja.jorm.patch.postgres.FixedPGmoney;
import com.jajja.jorm.patch.postgres.FixedPGobject;

public abstract class Patcher {
    private static HashMap<Class<?>, Patcher> patchers = new HashMap<Class<?>, Patcher>(2);
    private static Patcher noopPatcher = new NoopFulpatcher();

    static {
        try {
            Class.forName("org.postgresql.util.PGobject");
            patchers.put(PGobject.class, new PGobjectFulpatcher());
        } catch (Exception e) {
        }
        try {
            Class.forName("org.postgresql.util.PGmoney");
            patchers.put(PGmoney.class, new PGmoneyFulpatcher());
        } catch (Exception e) {
        }
    }

    private Patcher() { }

    protected abstract Object debork(Object v);


    public static class NoopFulpatcher extends Patcher {
        private NoopFulpatcher() {
        }

        @Override
        protected Object debork(Object v) {
            return v;
        }
    }

    private static class PGobjectFulpatcher extends Patcher {
        private PGobjectFulpatcher() throws Exception {
            PGobject a = new PGobject();
            PGobject b = new PGobject();
            a.setType("a");
            b.setType("b");
            a.setValue("test");
            b.setValue("test");
            if (a.equals(b)) {  // should not equal; type differs
                return;
            }
            b.setType("a");
            if (a.hashCode() != b.hashCode()) {
                return;
            }
        }

        @Override
        protected Object debork(Object v) {
            return new FixedPGobject((PGobject)v);
        }
    }

    private static class PGmoneyFulpatcher extends Patcher {
        private PGmoneyFulpatcher() throws Exception {
            PGmoney a = new PGmoney(12.34);
            PGmoney b = new PGmoney(12.34);
            if (a.hashCode() != b.hashCode()) {
                throw new Exception("bugged");
            }
        }

        @Override
        protected Object debork(Object v) {
            return new FixedPGmoney((PGmoney)v);
        }
    }

    public static Patcher get(Class<?> clazz) {
        Patcher p = patchers.get(clazz);
        return (p == null) ? noopPatcher : p;
    }

    public static Object unbork(Object o) {
        if (o != null) {
            o = get(o.getClass()).debork(o);
        }
        return o;
    }
}
