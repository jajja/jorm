/*
 * Copyright (C) 2014 Jajja Communications AB
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
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
