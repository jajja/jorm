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

/**
 *
 * @see Jorm
 * @see Record
 * @author Daniel Adolfsson <daniel.adolfsson@jajja.com>
 * @author Andreas Allerdahl <andreas.allerdahl@jajja.com>
 * @author Martin Korinth <martin.korinth@jajja.com>
 * @since 1.0.0
 */
public class Table {
    private static Map<Class<?>, Table> map = new ConcurrentHashMap<Class<?>, Table>(16, 0.75f, 1);
    private String database;
    private String schema;
    private String table;
    private Composite primaryKey;
    private String immutablePrefix;

    public static Table get(Class<? extends Record> clazz) {
        Table table = map.get(clazz);
        if (table == null) {
            synchronized (map) {
                table = map.get(clazz);
                if (table == null) {
                    table = new Table(clazz);
                    map.put(clazz, table);
                }
            }
        }
        return table;
    }

    public Table(String database) {
        this.database = database;
    }

    private Table(Class<? extends Record> clazz) {
        Jorm jorm = clazz.getAnnotation(Jorm.class);
        if (jorm == null) {
            throw new RuntimeException("Jorm annotation missing in " + clazz);
        }
        if (jorm.table().equals("") ^ jorm.primaryKey().length == 0) {
            throw new RuntimeException("Tables cannot be mapped without primary keys. Either define both table and primary key or none in the Jorm annotation.");
        }
        database = jorm.database();
        schema = nullify(jorm.schema());
        table = nullify(jorm.table());
        if (table != null) {
            primaryKey = new Composite(jorm.primaryKey());
        }
        if (jorm.immutablePrefix().length() > 0) {
            this.immutablePrefix = jorm.immutablePrefix();
        } else {
            immutablePrefix = null;
        }
    }

    private static String nullify(String string) {
        return string.isEmpty() ? null : string;
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

    public boolean isImmutable(Symbol symbol) {
        return immutablePrefix != null && symbol.getName().startsWith(immutablePrefix);
    }

    public String getImmutablePrefix() {
        return immutablePrefix;
    }
}
