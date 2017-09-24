/*
 * Copyright (C) 2013 Jajja Communications AB
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
package com.jajja.jorm;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.jajja.jorm.Row.NamedField;

/**
 *
 * @see Jorm
 * @see Record
 * @author Daniel Adolfsson &lt;daniel.adolfsson@jajja.com&lt;
 * @author Andreas Allerdahl &lt;andreas.allerdahl@jajja.com&lt;
 * @author Martin Korinth &lt;martin.korinth@jajja.com&lt;
 * @since 1.0.0
 */
public class Table {
    private static Map<Class<?>, Table> map = new ConcurrentHashMap<Class<?>, Table>(16, 0.75f, 1);
    private final String database;
    private final String schema;
    private final String table;
    private final Composite primaryKey;
    private final String immutablePrefix;

    public static Table get(Class<? extends Record> clazz) {
        Table table = map.get(clazz);
        if (table == null) {
            synchronized (map) {
                table = map.get(clazz);
                if (table == null) {
                    table = new Table(tableAnnotation(clazz));
                    if (table.getTable() == null ^ table.getPrimaryKey() == null) {
                        throw new RuntimeException("Tables cannot be mapped without primary keys. Either define both table and primary key or neither in the Jorm annotation.");
                    }
                    map.put(clazz, table);
                }
            }
        }
        return table;
    }

    public Table(String database, String schema, String table, Composite primaryKey, String immutablePrefix) {
        this.database = database;
        this.schema = schema;
        this.table = table;
        this.primaryKey = primaryKey;
        this.immutablePrefix = immutablePrefix != null ? immutablePrefix : Jorm.NONE;
    }

    public Table(JormAnnotation annotation) {
        this.database = Jorm.NONE.equals(annotation.database) ? null : annotation.database;
        this.schema = Jorm.NONE.equals(annotation.schema) ? null : annotation.schema;
        this.table = Jorm.NONE.equals(annotation.table) ? null : annotation.table;
        if (annotation.primaryKey == null || annotation.primaryKey.length == 0 || Jorm.NONE.equals(annotation.primaryKey[0])) {
            this.primaryKey = null;
        } else {
            this.primaryKey = new Composite(annotation.primaryKey);
        }
        if (annotation.immutablePrefix == null) {
            this.immutablePrefix = "__";
        } else if (annotation.immutablePrefix.equals(Jorm.NONE)) {
            this.immutablePrefix = "\0";  // no column names start with "\0" :P
        } else {
            this.immutablePrefix = annotation.immutablePrefix;
        }
    }

    private static JormAnnotation tableAnnotation(Class<?> clazz) {
        if (!Record.isRecordSubclass(clazz)) {
            return null;
        }

        Jorm jorm = clazz.getAnnotation(Jorm.class);
        JormAnnotation superAnnotation = tableAnnotation(clazz.getSuperclass());

        if (jorm == null) {
            return superAnnotation;
        }

        JormAnnotation annotation = new JormAnnotation(jorm);

        return annotation.merge(superAnnotation);
    }

    public String getDatabase() {
        return database;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public Composite getPrimaryKey() {
        return primaryKey;
    }

    public boolean isImmutable(String column) {
        return column.startsWith(immutablePrefix);
    }

    public boolean isImmutable(NamedField f) {
        return f.name().startsWith(immutablePrefix);
    }

    public String getImmutablePrefix() {
        return immutablePrefix;
    }

    @Override
    public String toString() {
        return String.format("Table [database => %s, schema => %s, table => %s, primaryKey => %s, immutablePrefix => %s]", database, schema, table, primaryKey, immutablePrefix);
    }

    private static class JormAnnotation {
        String database;
        String schema;
        String table;
        String[] primaryKey;
        String immutablePrefix;

        private static String nullify(String s) {
            return Jorm.INHERIT.equals(s) ? null : s;
        }

        private static String[] nullify(String[] s) {
            return s.length == 0 || (s.length == 1 && Jorm.INHERIT.equals(s[0])) ? null : s;
        }

        public JormAnnotation(Jorm jorm) {
            this.database = nullify(jorm.database());
            this.schema = nullify(jorm.schema());
            this.table = nullify(jorm.table());
            this.primaryKey = nullify(jorm.primaryKey());
            this.immutablePrefix = nullify(jorm.immutablePrefix());
        }

        public JormAnnotation merge(JormAnnotation superAnnotation) {
            if (superAnnotation != null) {
                database = (database == null) ? superAnnotation.database : database;
                schema = (schema == null) ? superAnnotation.schema : schema;
                table = (table == null) ? superAnnotation.table : table;
                primaryKey = primaryKey == null ? superAnnotation.primaryKey : primaryKey;
                immutablePrefix = (immutablePrefix == null) ? superAnnotation.immutablePrefix : immutablePrefix;
            }
            return this;
        }
    }
}
