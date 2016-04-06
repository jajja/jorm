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

import java.util.Arrays;

import com.jajja.jorm.Row.Column;

public class Composite {
    private final Symbol[] symbols;
    private int hashCode = 0;

    public Composite(String ... columns) {
        Symbol[] symbols = new Symbol[columns.length];
        for (int i = 0; i < columns.length; i++) {
            symbols[i] = Symbol.get(columns[i]);
        }
        this.symbols = tidy(symbols, false);
    }

    public Composite(Symbol ... symbols) {
        this.symbols = tidy(symbols, true);
    }

    public static Composite get(Object o) {
        if (o instanceof String) {
            return new Composite((String)o);
        } else if (o instanceof Symbol) {
            return new Composite((Symbol)o);
        } else if (o instanceof Composite) {
            return (Composite)o;
        }
        throw new IllegalArgumentException();
    }

    private static Symbol[] tidy(Symbol[] symbols, boolean copy) {
        int len = symbols.length;
        if (len == 0) {
            throw new IllegalArgumentException("At least 1 symbol is required");
        }
        if (len > 1) {
            Arrays.sort(symbols);
            int n = 0;
            for (int i = 0; i < len; i++) {
                symbols[n++] = symbols[i];
                while (i < len - 1 && symbols[i].identity == symbols[i+1].identity) {
                    i++;
                }
            }
            copy = (len != n);
            len = n;
        }
        if (copy) {
            symbols = Arrays.copyOf(symbols, len);
        }
        return symbols;
    }

    public Value valueFrom(Row row) {
        Object[] values = new Object[symbols.length];
        for (int i = 0; i < symbols.length; i++) {
            values[i] = row.get(symbols[i]);
        }
        return new Value(this, values);
    }

    public Value valueFrom(Row row, boolean noRefresh) {
        if (!noRefresh) {
            return valueFrom(row);
        }
        Object[] values = new Object[symbols.length];
        for (int i = 0; i < symbols.length; i++) {
            Column column = row.columns.get(symbols[i]);
            if (column == null) {
                throw new NullPointerException("Column " + symbols[i].getName() + " is not set");
            }
            values[i] = column.dereference();
        }
        return new Value(this, values);
    }

    public Value value(Object ... values) {
        if (this.symbols.length != values.length) {
            throw new IllegalArgumentException("Argument count must equal the number of columns (" + symbols.length + ")");
        }
        return new Value(this, values);
    }

    public Symbol[] getSymbols() {
        return symbols;
    }

    public Symbol getSymbol() {
        if (!isSingle()) {
            throw new RuntimeException("isSingle() == false");
        }
        return symbols[0];
    }

    public boolean isSingle() {
        return symbols.length == 1;
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

        private int getOffset(Symbol symbol) {
            for (int i = 0; i < composite.symbols.length; i++) {
                if (composite.symbols[i].equals(symbol)) {
                    return i;
                }
            }
            return -1;
        }

        public <T> T get(String column, Class<T> clazz) {
            return get(Symbol.get(column), clazz);
        }

        @SuppressWarnings("unchecked")
        public <T> T get(Symbol symbol, Class<T> clazz) {
            int offset = getOffset(symbol);
            if (offset < 0) {
                throw new IllegalArgumentException("No such column " + symbol);
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
        if (hashCode == 0) {
            for (Symbol symbol : symbols) {
                hashCode = hashCode * 31 + symbol.hashCode();
            }
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Composite) {
            Composite c = (Composite)obj;
            if (symbols.length != c.symbols.length) {
                return false;
            }
            for (int i = 0; i < symbols.length; i++) {
                if (symbols[i].identity != c.symbols[i].identity) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public boolean contains(Symbol symbol) {
        for (Symbol s : symbols) {
            if (s.equals(symbol)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(Math.min(16, 12 * symbols.length));
        sb.append("{");
        boolean isFirst = true;
        for (Symbol symbol : symbols) {
            if (!isFirst) {
                sb.append(", ");
            } else {
                isFirst = false;
            }
            sb.append(symbol);
        }
        sb.append("}");
        return sb.toString();
    }
}
