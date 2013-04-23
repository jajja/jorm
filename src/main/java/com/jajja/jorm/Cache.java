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

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A record cache implementation, using LRU.
 * 
 * @see Record
 * @author Andreas Allerdahl <andreas.allerdahl@jajja.com>
 * @since 1.0.0
 */
public class Cache<C extends Record> {
    private Lru<Object> map;
    private Class<C> clazz;
    private HashSet<String> additionalColumns = new HashSet<String>();
    private String id;
    // <columnValue, primaryKeyValue>
    private Map<String, Map<Object, Object>> additionalMap = new HashMap<String, Map<Object, Object>>();

    private class Lru<K> extends LinkedHashMap<K,C> {
        private static final long serialVersionUID = 1L;
        int capacity;
        Lru(int capacity) {
            super(capacity, 1.0f, true);
            this.capacity = capacity;
        }
        @Override
        public boolean removeEldestEntry(Map.Entry<K,C> eldest) {
            if (size() > capacity) {
                deindex(eldest.getValue());
                return true;
            }
            return false;
        }
    }

    public Cache(int capacity, Class<C> clazz) {
        map = new Lru<Object>(capacity);

        this.clazz = clazz;  // C.class!
        id = Table.get(clazz).getId().getName();
    }

    public void indexAdditionalColumn(String column) {
        synchronized (map) {
            if (!map.isEmpty()) {
                throw new RuntimeException("cache must be empty");
            }

            additionalColumns.add(column);
            additionalMap.put(column, new HashMap<Object, Object>());
        }
    }

    protected boolean fetchInto(String column, Object value, C record) throws SQLException {
        return record.populateByColumn(column, value);
    }

    public void put(Collection<C> records) {
        synchronized (map) {
            for (C record : records) {
                Object key = record.id();
                if (key != null) {
                    index(record);
                }
            }
        }
    }

    private void index(C record) {
        if (map.containsKey(record.id())) {
            touch(record.id());
            return;
        }
        map.put(record.id(), record);
        for (String column : additionalColumns) {
            Map<Object, Object> amap = additionalMap.get(column);
            Object value = record.get(column);
            if (amap.containsKey(value)) {
                throw new RuntimeException("collision! column '" + column + "' already contains value '" + value + "' (while indexing record " + record + ")");
            }
            amap.put(value, record.id());
        }
    }

    private void deindex(C record) {
        for (String column : additionalColumns) {
            Map<Object, Object> amap = additionalMap.get(column);
            Object value = record.get(column);
            if (!amap.containsKey(value)) {
                throw new RuntimeException("index corruption! column '" + column + "' no longer contains value '" + value + "'");
            }
            amap.remove(value);
        }
    }

    public C get(String column, Object value) {
        synchronized (map) {
            C record;

            if (id.equals(column)) {
                record = map.get(value);
            } else {
                if (!additionalColumns.contains(column)) {
                    throw new IllegalArgumentException("column '" + column + "' is not indexed");
                }
                Map<Object, Object> amap = additionalMap.get(column);
                record = map.get( amap.get(value) );
            }

            if (record == null) {
                try {
                    record = clazz.newInstance();
                } catch (Exception e) {
                    throw new RuntimeException("failed to create new instance", e);
                }
                try {
                    if (!fetchInto(column, value, record)) {
                        //  TODO negative cache?
                        record = null;
                    }
                } catch (SQLException e) {
                    throw new RuntimeException("failed to fetch record by key " + value, e);
                }
                if (record != null) {
                    index(record);
                }
            } else {
                touch(value);
            }
            return record;
        }
    }

    public C get(Object value) {
        return get(id, value);
    }

    public void put(C record) {
        synchronized (map) {
            index(record);
        }
    }

    public void remove(Object key) {
        synchronized (map) {
            if (map.containsKey(key)) {
                deindex(map.remove(key));
            }
        }
    }

    public void touch(Object key) {
        synchronized (map) {
            if (map.containsKey(key)) {
                map.put(key, map.remove(key));
            }
        }
    }

    public void clear() {
        synchronized (map) {
            map.clear();
            for (String column : additionalColumns) {
                Map<Object, Object> map = additionalMap.get(column);
                map.clear();
            }
        }
    }
}
