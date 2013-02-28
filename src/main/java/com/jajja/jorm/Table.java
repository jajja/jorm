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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
    private static Map<Class<?>, Table> map = new HashMap<Class<?>, Table>();
    private String database;
    private String schema;
    private String table;
    private Symbol id;
    private Set<Symbol> immutable;
    
    public static synchronized Table get(Class<? extends Record> clazz) {
        Table table = map.get(clazz);
        if (table == null) {
            table = new Table(clazz);
            map.put(clazz, table);
        }
        return table;
    }

    public Table(String database) {
        this.database = database;
    }
    
    private Table(Class<? extends Record> clazz) {
        Jorm jorm = clazz.getAnnotation(Jorm.class);
        if (jorm == null) {
        	throw new RuntimeException("Jorm annotation missing in " + Jorm.class);
        }
        if (jorm.table().equals("") ^ jorm.id().equals("")) {
            throw new RuntimeException("Tables cannot be mapped without primary keys. Either define both table and primary key or none in the Jorm annotation.");
        }
        database = jorm.database();
        schema = nullify(jorm.schema());
        table = nullify(jorm.table());
        if (table != null) {
            id = Symbol.get(jorm.id());
        }
        if (jorm.immutable().length > 0) {
            immutable = new HashSet<Symbol>();
            for (String column : jorm.immutable()) {
                immutable.add(Symbol.get(column));
            }
        } else {
            immutable = null;
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

    public Symbol getId() {
        return id;
    }
    
    public boolean isImmutable(Symbol symbol) {
        return immutable != null && immutable.contains(symbol);
    }
    
    Query getSelectQuery(Dialect dialect) {
        if (table != null) { // XXX: fit in timeline to extend mapping to generic SQL
            return new Query(dialect, "SELECT * FROM #1# ", this);
        } else {
            throw new RuntimeException("Cannot construct select query without either table or sql definition!");
        }
    }
}
