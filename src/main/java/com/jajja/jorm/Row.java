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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.jajja.jorm.Composite.Value;
import com.jajja.jorm.patch.Patcher;

/**
 * @see Transaction
 * @see Record
 * @author Andreas Allerdahl &lt;andreas.allerdahl@jajja.com&gt;
 * @author Martin Korinth &lt;martin.korinth@jajja.com&gt;
 * @author Daniel Adolfsson &lt;daniel.adolfsson@jajja.com&gt;
 * @since 2.0.0
 */
public class Row {
    public static final byte FLAG_STALE = 0x01;
    public static final byte FLAG_READ_ONLY = 0x02;
    public static final byte FLAG_REF_FETCH = 0x04;
    byte flags = FLAG_REF_FETCH;
    Map<Symbol, Column> columns = new HashMap<Symbol, Column>(8, 1.0f);

    public static class Column {
        private Object value = null;
        private boolean isChanged = false;

        Column() {}

        void setValue(Object value) {
            this.value = Patcher.unbork(value);
        }

        public Object rawValue() {
            return value;
        }

        public Record record() {
            return (value instanceof Record) ? ((Record)value) : null;
        }

        public Object dereference() {
            return (value instanceof Record) ? ((Record)value).id() : value;
        }

        void setChanged(boolean isChanged) {
            this.isChanged = isChanged;
        }

        boolean isChanged() {
            return isChanged;
        }

        @Override
        public String toString() {
            return String.format("Column [value => %s, isChanged => %s]", value, isChanged);
        }
    }

    public Row() {
    }

    public void set(Row row) {
        for (Entry<Symbol, Column> entry : row.columns.entrySet()) {
            put(entry.getKey(), entry.getValue().rawValue()); // XXX: null safe?
        }
    }

    Column getOrCreateColumn(Symbol symbol) {
        Column column = columns.get(symbol);
        if (column == null) {
            column = new Column();
            columns.put(symbol, column);
        }
        return column;
    }


    /**
     * Reinitializes the Row to optimally hold {@code size} column values.
     * All column values are cleared, and the row is marked not stale.
     *
     * @param size the number of columns
     */
    public void resetColumns(int size) {
        columns = new HashMap<Symbol, Column>(size, 1.0f);
        stale(false);
    }

    /**
     * Provides an immutable view of the columns.
     *
     * @return the columns
     */
    public Map<Symbol, Column> columns() {
        return Collections.unmodifiableMap(columns);
    }

    /**
     * Checks whether any of the values of the columns specified by the composite key are null or changed.
     *
     * @param key a composite key
     * @return true if any of the column values are null or changed since the last call to populate()
     */
    public boolean isCompositeKeyNullOrChanged(Composite key) {
        assertNotStale();
        for (Symbol symbol : key.getSymbols()) {
            Column column = columns.get(symbol);
            if (column == null || column.dereference() == null || column.isChanged()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks whether any of the values of the columns specified by the composite key are null.
     *
     * @param key a composite key
     * @return true if any of the column values are null
     */
    public boolean isCompositeKeyNull(Composite key) {
        assertNotStale();
        for (Symbol symbol : key.getSymbols()) {
            Column column = columns.get(symbol);
            if (column == null || column.dereference() == null) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks whether the column is set or not.
     *
     * This is the equivalent of calling isSet(Symbol.get(column))
     *
     * @param column the name of the column
     * @return true if the column is set, false otherwise
     */
    public boolean isSet(String column) {
        return isSet(Symbol.get(column));
    }

    /**
     * Checks whether the column is set or not.
     *
     * @param symbol the name of the column
     * @return true if the column is set, false otherwise
     */
    public boolean isSet(Symbol symbol) {
        assertNotStale();
        return columns.get(symbol) != null;
    }

    /**
     * Checks whether the column value has changed since the last call to populate().
     *
     * This is the equivalent of calling isChanged(Symbol.get(column))
     *
     * @param symbol the column name
     * @return true if the column value has changed, false otherwise
     */
    public boolean isChanged(String column) {
        return isChanged(Symbol.get(column));
    }

    /**
     * Checks whether the column value has changed since the last call to populate().
     *
     * @param symbol the column name
     * @return true if the column value has changed, false otherwise
     */
    public boolean isChanged(Symbol symbol) {
        Column column = columns.get(symbol);
        if (column == null) {
            return false;
        }
        return column.isChanged();
    }

    /**
     * Checks whether any column values have changed since the last call to populate().
     *
     * @return true if at least one column value has changed, false otherwise
     */
    public boolean isChanged() {
        for (Column column : columns.values()) {
            if (column.isChanged()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Marks all columns as changed.
     */
    public void taint() {
        for (Entry<Symbol, Column> entry : columns.entrySet()) {
            Column column = entry.getValue();
            column.setChanged(true);
        }
    }

    /**
     * Marks all columns as unchanged.
     */
    public void purify() {
        for (Column column : columns.values()) {
            column.setChanged(false);
        }
    }

    private void flag(int flag, boolean set) {
        if (set) {
            flags |= flag;
        } else {
            flags &= ~flag;
        }
    }

    private static void flag(Collection<? extends Row> rows, int flag, boolean set) {
        for (Row row : rows) {
            row.flag(flag, set);
        }
    }

    private boolean flag(int flag) {
        return (flags & flag) != 0;
    }

    /**
     * Changes the row's staleness.
     *
     * @param setStale whether or not the row should be marked stale
     */
    public void stale(boolean setStale) {
        flag(FLAG_STALE, setStale);
    }

    public static void stale(Collection<? extends Row> rows, boolean setStale) {
        flag(rows, FLAG_STALE, setStale);
    }

    /**
     * Checks whether the row is stale.
     *
     * @return true if the row is stale, false otherwise.
     */
    public boolean isStale() {
        return flag(FLAG_STALE);
    }

    /**
     * Asserts that the row is not stale.
     */
    public void assertNotStale() {
        if (isStale()) {
            throw new RuntimeException("Row is stale!");
        }
    }

    /**
     * Changes the row's read-onlyness.
     *
     * @param setReadOnly whether or not the row should be marked read-only
     */
    public void readOnly(boolean setReadOnly) {
        flag(FLAG_READ_ONLY, setReadOnly);
    }

    public static void readOnly(Collection<? extends Row> rows, boolean setStale) {
        flag(rows, FLAG_READ_ONLY, setStale);
    }

    /**
     * Checks whether the row is read-only.
     *
     * @return true if the row is read-only, false otherwise.
     */
    boolean isReadOnly() {
        return flag(FLAG_READ_ONLY);
    }

    /**
     * Asserts that the row is not read-only.
     */
    public void assertNotReadOnly() {
        if (isReadOnly()) {
            throw new RuntimeException("Row is read only!");
        }
    }

    public void refFetch(boolean refFetch) {
        flag(FLAG_REF_FETCH, refFetch);
    }

    public static void refFetch(Collection<? extends Row> rows, boolean setStale) {
        flag(rows, FLAG_REF_FETCH, setStale);
    }

    public boolean isRefFetch() {       // Best name 2015
        return flag(FLAG_REF_FETCH);
    }

    public void unmodifiable() {
        flag(FLAG_READ_ONLY, true);
        flag(FLAG_REF_FETCH, false);
    }

    void put(Symbol symbol, Object value) {
        Column column = columns.get(symbol);
        if (column == null) {
            column = new Column();
        }

        column.setChanged(true);
        column.setValue(value);

        columns.put(symbol, column);
    }

    /**
     * Sets the specified column to value.
     *
     * If the value refers to a {@link Record} it is stored as a reference,
     * and the column is assigned the value of the Record's primary key.
     *
     * This is the equivalent of calling set(Symbol.get(column), value)
     *
     * @param column the column name
     * @param value  the value
     */
    public void set(String column, Object value) {
        set(Symbol.get(column), value);
    }

    /**
     * Sets the specified column to value.
     *
     * If the value refers to a {@link Record} it is stored as a reference,
     * and the column is assigned the value of the Record's primary key.
     *
     * @param symbol the column name
     * @param value  the value
     */
    public void set(Symbol symbol, Object value) {
        assertNotReadOnly();
        put(symbol, value);
    }

    /**
     * Unsets the specified column.
     *
     * This is the equivalent of calling unset(Symbol.get(column))
     *
     * @param column the column name
     */
    public void unset(String column) {
        unset(Symbol.get(column));
    }

    /**
     * Unsets the specified column.
     *
     * @param symbol the column name
     */
    public void unset(Symbol symbol) {
        assertNotReadOnly();
        assertNotStale();

        Column column = columns.get(symbol);
        if (column != null) {
            columns.remove(symbol);
        }
    }

    /**
     * Constructs a {@link Value} with the values of the columns specified by the composite key.
     *
     * @param composite the composite key
     * @return a {@link Value} filled with the values of the columns specified by the composite key
     */
    public Value get(Composite composite) {
        return composite.valueFrom(this);
    }

    /**
     * Provides the value of a column.
     *
     * This is the equivalent of calling get(Symbol.get(column))
     *
     * @param column the column name
     * @return the column value
     * @throws RuntimeException if the column is not set
     */
    public Object get(String column) {
        return get(Symbol.get(column));
    }

    /**
     * Provides the value of a column.
     *
     * @param symbol the column name
     * @return the column value
     * @throws RuntimeException if the column is not set
     */
    public Object get(Symbol symbol) {
        try {
            return getColumnValue(symbol, Object.class, false, false, null);
        } catch (SQLException e) {
            // UNREACHABLE
            throw new IllegalStateException(e);
        }
    }

    /**
     * Provides a type-casted value of a column. A runtime exception is thrown if the
     * column value can not be cast to the specified type.
     *
     * This is the equivalent of calling get(Symbol.get(column), clazz)
     *
     * @param column the column name.
     * @param clazz the expected class
     * @return the type-casted column value
     * @throws RuntimeException if the column is not set
     */
    public <T> T get(String column, Class<T> clazz) {
        return get(Symbol.get(column), clazz);
    }

    /**
     * Provides a type-casted value of a column. A runtime exception is thrown if the
     * column value can not be cast to the specified type.
     *
     * This is the equivalent of calling get(Symbol.get(column), clazz)
     *
     * @param column the column name.
     * @param clazz the expected class
     * @return the type-casted column value
     * @throws RuntimeException if the column is not set
     */
    public <T> T get(Symbol symbol, Class<T> clazz) {
        try {
            return getColumnValue(symbol, clazz, false, false, null);
        } catch (SQLException e) {
            // UNREACHABLE
            throw new IllegalStateException(e);
        }
    }

    /**
     * Provides a column Record reference (foreign key reference). If the reference is cached
     * (previously fetched), the cached Record object is returned, otherwise a database SELECT
     * is executed in the Record's default transaction to map the database row to a Record.
     *
     * To specify a transaction, use ref(column, clazz, transaction).
     *
     * This method is identical to get(column, clazz), except it can throw {@link SQLException}s.
     * This is the equivalent of calling:
     *   ref(column, clazz, null)
     *   ref(Symbol.get(column), clazz)
     *   ref(Symbol.get(column), clazz, null)
     *
     * @param column the column name
     * @param clazz the Record class to reference
     * @return a Record reference
     * @throws RuntimeException if the column is not set
     * @throws SQLException
     */
    public <T extends Record> T ref(String column, Class<T> clazz) throws SQLException {
        return ref(Symbol.get(column), clazz, null);
    }

    /**
     * Provides a column Record reference (foreign key reference). If the reference is cached
     * (previously fetched), the cached Record object is returned, otherwise a database SELECT
     * is executed in the Record's default transaction to map the database row to a Record.
     *
     * To specify a transaction, use ref(symbol, clazz, transaction).
     *
     * This method is identical to get(column, clazz), except it can throw {@link SQLException}s.
     *
     * This is the equivalent of calling ref(symbol, clazz, null)
     *
     * @param column the column name
     * @param clazz the Record class to reference
     * @return a Record reference
     * @throws RuntimeException if the column is not set
     * @throws SQLException
     */
    public <T extends Record> T ref(Symbol symbol, Class<T> clazz) throws SQLException {
        return ref(symbol, clazz, null);
    }

    /**
     * Provides a column Record reference (foreign key reference). If the reference is cached
     * (previously fetched), the cached Record object is returned, otherwise a database SELECT
     * is executed in the specified transaction to map the database row to a Record.
     *
     * This method is identical to get(column, clazz), except it can throw {@link SQLException}s.
     *
     * This is the equivalent of calling ref(Symbol.get(column), clazz, transaction)
     *
     * @param column the column name
     * @param clazz the Record class to reference
     * @param transaction the transaction to use for SELECTs, or null to use the Record's default
     * @return a Record reference
     * @throws RuntimeException if the column is not set
     * @throws SQLException
     */
    public <T extends Record> T ref(String column, Class<T> clazz, Transaction transaction) throws SQLException {
        return ref(Symbol.get(column), clazz, transaction);
    }

    /**
     * Provides a column Record reference (foreign key reference). If the reference is cached
     * (previously fetched), the cached Record object is returned, otherwise a database SELECT
     * is executed in the specified transaction to map the database row to a Record.
     *
     * This method is identical to get(column, clazz), except it can throw {@link SQLException}s.
     *
     * @param column the column name
     * @param clazz the Record class to reference
     * @param transaction the transaction to use for SELECTs, or null to use the Record's default
     * @return a Record reference
     * @throws RuntimeException if the column is not set
     * @throws SQLException
     */
    public <T extends Record> T ref(Symbol symbol, Class<T> clazz, Transaction transaction) throws SQLException {
        return getColumnValue(symbol, clazz, false, true, transaction);
    }

    /**
     * Provides a cached (previously fetched) column Record reference (foreign key reference).
     * Unlike the ref() methods, this method does not attempt to fetch any data from the database.
     *
     * This is the equivalent of calling refCached(Symbol.get(column), clazz)
     *
     * @param column the column name
     * @param clazz the Record class to reference
     * @return a cached Record reference, or null if there is no cached reference
     * @throws RuntimeException if the column is not set
     */
    public <T extends Record> T refCached(String column, Class<T> clazz) {
        return refCached(Symbol.get(column), clazz);
    }

    /**
     * Provides a cached (previously fetched) column Record reference (foreign key reference).
     * Unlike the ref() methods, this method does not attempt to fetch any data from the database.
     *
     * @param symbol the column name
     * @param clazz the Record class to reference
     * @return a cached Record reference, or null if there is no cached reference
     * @throws RuntimeException if the column is not set
     */
    public <T extends Record> T refCached(Symbol symbol, Class<T> clazz) {
        try {
            return getColumnValue(symbol, clazz, true, false, null);
        } catch (SQLException e) {
            // UNREACHABLE
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("unchecked")
    <T> T getColumnValue(Symbol symbol, Class<T> clazz, boolean isReferenceCacheOnly, boolean throwSqlException, Transaction transaction) throws SQLException {
        assertNotStale();

        Column column = columns.get(symbol);
        if (column == null) {
            throw new RuntimeException("Column '" + symbol.getName() + "' does not exist, or has not yet been set on " + this);
        }

        Object value = column.rawValue();

        if (value != null) {
            if (Record.isRecordSubclass(clazz)) {
                Record record = column.record();
                if (record == null) {
                    // Load foreign key
                    if (isReferenceCacheOnly) {
                        return null;
                    }
                    if (!flag(FLAG_REF_FETCH)) {
                        throw new IllegalAccessError("Reference fetching is disabled");
                    }
                    try {
                        transaction = (transaction != null ? transaction : Record.transaction((Class<? extends Record>)clazz));
                        record = transaction.findById((Class<? extends Record>)clazz, value);
                        column.setValue(record);
                    } catch (SQLException e) {
                        if (throwSqlException) {
                            throw e;
                        }
                        throw new RuntimeException("Failed to findById(" + clazz + ", " + value + ")", e);
                    }
                } else if (!record.getClass().equals(clazz)) {
                    throw new IllegalArgumentException("Class mismatch " + clazz + " != " + record.getClass());
                }
                value = record;
            } else {
                try {
                    value = convert(value, clazz);
                } catch (IllegalArgumentException e) {
                    throw new RuntimeException("Column " + symbol.getName() + ": " + e.getMessage());
                }
            }
        }

        return (T)value;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        boolean isFirst = true;

        if (isStale()) {
            stringBuilder.append("stale ");
        }
        if (isReadOnly()) {
            stringBuilder.append("read-only ");
        }

        stringBuilder.append("Row { ");

        for (Entry<Symbol, Column> entry : columns.entrySet()) {
            if (isFirst) {
                isFirst = false;
            } else {
                stringBuilder.append(", ");
            }
            stringBuilder.append(entry.getKey().getName());
            stringBuilder.append(" => ");
            stringBuilder.append(entry.getValue().rawValue());
        }
        stringBuilder.append(" }");

        return stringBuilder.toString();
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof Row) {
            return columns.equals(((Row)object).columns);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return columns.hashCode();
    }

    private static void convertOverflow(Number n, Class<?> clazz) {
        throw new ArithmeticException(String.format("%s can not hold value %s", clazz.getSimpleName(), n.toString()));
    }

    private static final BigDecimal floatMaxValue = new BigDecimal(Float.MAX_VALUE);
    private static final BigDecimal doubleMaxValue = new BigDecimal(Double.MAX_VALUE);

    public static Number convertNumber(Number n, Class<?> clazz) {
        if (clazz.isInstance(n)) {
            return n;
        } else if (Integer.class.equals(clazz)) {
            long v = n.longValue();
            if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
                convertOverflow(n, clazz);
            }
            return n.intValue();
        } else if (Long.class.equals(clazz)) {
            double v = n.doubleValue();
            if (v < Long.MIN_VALUE || v > Long.MAX_VALUE) {
                convertOverflow(n, clazz);
            }
            return n.longValue();
        } else if (Short.class.equals(clazz)) {
            int v = n.intValue();
            if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
                convertOverflow(n, clazz);
            }
            return n.intValue();
        } else if (Byte.class.equals(clazz)) {
            int v = n.intValue();
            if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
                convertOverflow(n, clazz);
            }
            return n.byteValue();
        } else if (Double.class.equals(clazz)) {
            if (n instanceof BigDecimal && ((BigDecimal)n).abs().compareTo(doubleMaxValue) > 0) {
                convertOverflow(n, clazz);
            } else if (n instanceof BigInteger && ((BigDecimal)n).abs().compareTo(doubleMaxValue) > 0) {
                convertOverflow(n, clazz);
            }
            return n.doubleValue();
        } else if (Float.class.equals(clazz)) {
            if (n instanceof BigDecimal && ((BigDecimal)n).abs().compareTo(floatMaxValue) > 0) {
                convertOverflow(n, clazz);
            } else if (n instanceof BigInteger && ((BigDecimal)n).abs().compareTo(floatMaxValue) > 0) {
                convertOverflow(n, clazz);
            }
            double v = n.doubleValue();
            if (!Double.isInfinite(v) && (v < -Float.MAX_VALUE || v > Float.MAX_VALUE)) {
                convertOverflow(n, clazz);
            }
            return n.floatValue();
        } else if (BigDecimal.class.equals(clazz)) {
            return new BigDecimal(n.toString());
        } else if (BigInteger.class.equals(clazz)) {
            return new BigInteger(n.toString());
        } else {
            throw new IllegalArgumentException(String.format("Cannot convert number of type %s to class %s", n.getClass(), clazz));
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T convert(Object value, Class<T> clazz) {
        if (value == null) {
            return null;
        }
        if (value instanceof Record) {
            value = ((Record)value).id();
        }
        if (value instanceof Composite.Value) {
            Composite.Value tmp = ((Composite.Value)value);
            if (tmp.isSingle()) {
                value = tmp.getValue();
            }
        }
        if (Number.class.isAssignableFrom(clazz) && value instanceof Number) {
            return (T)Row.convertNumber((Number)value, clazz);
        } else if (clazz.isAssignableFrom(value.getClass())) {
            return (T)value;
        } else {
            throw new IllegalArgumentException(String.format("Cannot convert object of type %s to class %s", value.getClass(), clazz));
        }
    }
}
