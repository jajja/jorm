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
package com.jajja.jorm;

import com.jajja.jorm.Row.Field;

public class Composite {
    private final String[] columns;
    private int hashCode = 0;
    private boolean hashCodeSet = false;

    public Composite(String ... columns) {
        this.columns = StringPool.array(columns);
    }

    public static Composite get(Object o) {
        if (o instanceof String) {
            return new Composite((String)o);
        } else if (o instanceof Composite) {
            return (Composite)o;
        }
        throw new IllegalArgumentException();
    }

    public Value valueFrom(Row row) {
        Object[] values = new Object[columns.length];
        for (int i = 0; i < columns.length; i++) {
            values[i] = row.get(columns[i]);
        }
        return new Value(this, values);
    }

    public Value valueFrom(Row row, boolean noRefresh) {
        if (!noRefresh) {
            return valueFrom(row);
        }
        Object[] values = new Object[columns.length];
        for (int i = 0; i < columns.length; i++) {
            Field field = row.field(columns[i]);
            if (field == null) {
                throw new NullPointerException("Column " + columns[i] + " is not set");
            }
            values[i] = field.dereference();
        }
        return new Value(this, values);
    }

    public Value value(Object ... values) {
        if (this.columns.length != values.length) {
            throw new IllegalArgumentException("Argument count must equal the number of columns (" + columns.length + ")");
        }
        return new Value(this, values);
    }

    public String[] getColumns() {
        return columns;
    }

    public String getColumn() {
        if (!isSingle()) {
            throw new RuntimeException("isSingle() == false");
        }
        return columns[0];
    }

    public boolean isSingle() {
        return columns.length == 1;
    }

    public static class Value {
        private Composite composite;
        private Object[] values;

        private Value(Composite composite, Object ... values) {
            this.composite = composite;
            this.values = values;
        }

        public Composite getComposite() {
            return composite;
        }

        public Object[] getValues() {
            return values;
        }

        public Object getValue() {
            if (!isSingle()) {
                throw new RuntimeException("Not a single column key");
            }
            return values[0];
        }

        private int getOffset(String column) {
            for (int i = 0; i < composite.columns.length; i++) {
                if (composite.columns[i].equals(column)) {
                    return i;
                }
            }
            return -1;
        }

        @SuppressWarnings("unchecked")
        public <T> T get(String column, Class<T> clazz) {
            int offset = getOffset(column);
            if (offset < 0) {
                throw new IllegalArgumentException("No such column " + column);
            }
            Object value = values[offset];
            if (value == null) {
                return null;
            }
            return (T)value;
        }

        public boolean isSingle() {
            return values.length == 1;
        }

        @Override
        public int hashCode() {
            int hashCode = 0;
            for (Object value : values) {
                if (value != null) {
                    hashCode += value.hashCode();
                }
            }
            return hashCode;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Value) {
                Value v = (Value)obj;
                if (values.length != v.values.length) {
                    return false;
                }
                for (int i = 0; i < values.length; i++) {
                    if (!((values[i] == v.values[i]) || (values[i] != null && values[i].equals(v.values[i])))) {
                        return false;
                    }
                }
                return true;
            }
            return false;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(16 * values.length);
            sb.append("{");
            boolean isFirst = true;
            for (Object value : values) {
                if (!isFirst) {
                    sb.append(", ");
                } else {
                    isFirst = false;
                }
                sb.append(value);
            }
            sb.append("}");
            return sb.toString();
        }
    }

    @Override
    public int hashCode() {
        if (!hashCodeSet) {
            for (String column : columns) {
                hashCode = hashCode * 31 + column.hashCode();
            }
            hashCodeSet = true;
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Composite) {
            Composite c = (Composite)obj;
            if (columns.length != c.columns.length) {
                return false;
            }
            for (int i = 0; i < columns.length; i++) {
                if (!columns[i].equals(c.columns[i])) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public boolean contains(String column) {
        for (String s : columns) {
            if (s.equals(column)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(Math.min(16, 12 * columns.length));
        sb.append("{");
        boolean isFirst = true;
        for (String column : columns) {
            if (!isFirst) {
                sb.append(", ");
            } else {
                isFirst = false;
            }
            sb.append(column);
        }
        sb.append("}");
        return sb.toString();
    }
}
