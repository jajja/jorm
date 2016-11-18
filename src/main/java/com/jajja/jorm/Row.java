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
    public static final byte FLAG_READ_ONLY = 0x02;
    public static final byte FLAG_REF_FETCH = 0x04;
    byte flags = FLAG_REF_FETCH;
    Map<String, Field> fields = new HashMap<String, Field>(8, 1.0f);

    public static class Field {
        private Object value = null;
        private boolean isChanged = false;

        Field() {}

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

        public boolean isChanged() {
            return isChanged;
        }

        @Override
        public String toString() {
            return String.format("Field [value => %s, isChanged => %s]", value, isChanged);
        }
    }

    public Row() {
    }

    public void set(Row row) {
        for (Entry<String, Field> entry : row.fields.entrySet()) {
            put(entry.getKey(), entry.getValue().rawValue());
        }
    }

    Field getOrCreateField(String column) {
        Field field = fields.get(column);
        if (field == null) {
            field = new Field();
            fields.put(StringPool.get(column), field);
        }
        return field;
    }


    /**
     * Reinitializes the Row to optimally hold {@code size} fields.
     * All existing fields are cleared.
     *
     * @param size the number of fields
     */
    public void resetFields(int size) {
        fields = new HashMap<String, Field>(size, 1.0f);
    }

    /**
     * Provides an immutable view of the fields.
     *
     * @return the fields
     */
    public Map<String, Field> fields() {
        return Collections.unmodifiableMap(fields);
    }

    /**
     * Checks whether any of the values of the fields specified by the composite key are null or changed.
     *
     * @param key a composite key
     * @return true if any of the fields are null or changed since the last call to populate()
     */
    public boolean isCompositeKeyNullOrChanged(Composite key) {
        for (String column : key.getColumns()) {
            Field field = fields.get(column);
            if (field == null || field.dereference() == null || field.isChanged()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks whether any of the values of the fields specified by the composite key are null.
     *
     * @param key a composite key
     * @return true if any of the fields are null
     */
    public boolean isCompositeKeyNull(Composite key) {
        for (String column : key.getColumns()) {
            Field field = fields.get(column);
            if (field == null || field.dereference() == null) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks whether the column is set or not.
     *
     * @param column the name of the column
     * @return true if the column is set, false otherwise
     */
    public boolean isSet(String column) {
        return fields.get(column) != null;
    }

    /**
     * Checks whether the column value has changed since the last call to populate().
     *
     * @param column the column name
     * @return true if the column value has changed, false otherwise
     */
    public boolean isChanged(String column) {
        Field field = fields.get(column);
        if (field == null) {
            return false;
        }
        return field.isChanged();
    }

    /**
     * Checks whether any fields have changed since the last call to populate().
     *
     * @return true if at least one field has changed, false otherwise
     */
    public boolean isChanged() {
        for (Field field : fields.values()) {
            if (field.isChanged()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Marks all fields as changed.
     */
    public void taint() {
        for (Entry<String, Field> entry : fields.entrySet()) {
            Field field = entry.getValue();
            field.setChanged(true);
        }
    }

    /**
     * Marks all fields as unchanged.
     */
    public void purify() {
        for (Field field : fields.values()) {
            field.setChanged(false);
        }
    }

    private void flag(int flag, boolean set) {
        if (set) {
            flags |= flag;
        } else {
            flags &= ~flag;
        }
    }

    private static void flag(Iterable<? extends Row> rows, int flag, boolean set) {
        for (Row row : rows) {
            row.flag(flag, set);
        }
    }

    private boolean flag(int flag) {
        return (flags & flag) != 0;
    }

    @Deprecated
    public void stale(boolean setStale) {
    }

    @Deprecated
    public static void stale(Iterable<? extends Row> rows, boolean setStale) {
    }

    @Deprecated
    public boolean isStale() {
        return false;
    }

    @Deprecated
    public void assertNotStale() {
    }

    /**
     * Changes the row's read-onlyness.
     *
     * @param setReadOnly whether or not the row should be marked read-only
     */
    public void readOnly(boolean setReadOnly) {
        flag(FLAG_READ_ONLY, setReadOnly);
    }

    public static void readOnly(Iterable<? extends Row> rows, boolean setReadOnly) {
        flag(rows, FLAG_READ_ONLY, setReadOnly);
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

    public static void refFetch(Iterable<? extends Row> rows, boolean setRefFetch) {
        flag(rows, FLAG_REF_FETCH, setRefFetch);
    }

    public boolean isRefFetch() {       // Best name 2015
        return flag(FLAG_REF_FETCH);
    }

    public void unmodifiable() {
        flag(FLAG_READ_ONLY, true);
        flag(FLAG_REF_FETCH, false);
    }

    void put(String column, Object value) {
        Field field = fields.get(column);
        if (field == null) {
            field = new Field();
            column = StringPool.get(column);
        }

        field.setChanged(true);
        field.setValue(value);

        fields.put(column, field);
    }

    /**
     * Sets the specified column to value.
     *
     * If the value refers to a {@link Record} it is stored as a reference,
     * and the column is assigned the value of the Record's primary key.
     *
     * @param column the column name
     * @param value  the value
     */
    public void set(String column, Object value) {
        assertNotReadOnly();
        put(column, value);
    }

    /**
     * Unsets the specified column.
     *
     * @param column the column name
     */
    public void unset(String column) {
        assertNotReadOnly();

        Field field = fields.get(column);
        if (field != null) {
            fields.remove(column);
        }
    }

    /**
     * Unsets the columns specified by the composite key.
     *
     * @param column the column name
     */
    public void unset(Composite composite) {
        assertNotReadOnly();

        for (String column : composite.getColumns()) {
            fields.remove(column);
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
     * @param column the column name
     * @return the column value
     * @throws RuntimeException if the column is not set
     */
    public Object get(String column) {
        try {
            return getColumnValue(column, Object.class, false, false, null);
        } catch (SQLException e) {
            // UNREACHABLE
            throw new IllegalStateException(e);
        }
    }

    /**
     * Provides a type-casted value of a column. A runtime exception is thrown if the
     * column value can not be cast to the specified type.
     *
     * @param column the column name.
     * @param clazz the expected class
     * @return the type-casted column value
     * @throws RuntimeException if the column is not set
     */
    public <T> T get(String column, Class<T> clazz) {
        try {
            return getColumnValue(column, clazz, false, false, null);
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
     *
     * This is the equivalent of calling ref(column, clazz, null)
     *
     * @param column the column name
     * @param clazz the Record class to reference
     * @return a Record reference
     * @throws RuntimeException if the column is not set
     * @throws SQLException
     */
    public <T extends Record> T ref(String column, Class<T> clazz) throws SQLException {
        return ref(column, clazz, null);
    }

    public Record ref(String column) throws SQLException {
        return ref(column, Record.class, null);
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
    public <T extends Record> T ref(String column, Class<T> clazz, Transaction transaction) throws SQLException {
        return getColumnValue(column, clazz, false, true, transaction);
    }

    /**
     * Provides a cached (previously fetched) column Record reference (foreign key reference).
     * Unlike the ref() methods, this method does not attempt to fetch any data from the database.
     *
     * @param column the column name
     * @param clazz the Record class to reference
     * @return a cached Record reference, or null if there is no cached reference
     * @throws RuntimeException if the column is not set
     */
    public <T extends Record> T refCached(String column, Class<T> clazz) {
        try {
            return getColumnValue(column, clazz, true, false, null);
        } catch (SQLException e) {
            // UNREACHABLE
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("unchecked")
    <T> T getColumnValue(String column, Class<T> clazz, boolean isReferenceCacheOnly, boolean throwSqlException, Transaction transaction) throws SQLException {
        Field field = fields.get(column);
        if (field == null) {
            throw new RuntimeException("Column '" + column + "' does not exist, or has not yet been set on " + this);
        }

        Object value = field.rawValue();

        if (value != null) {
            if (Record.isRecordSubclass(clazz)) {
                Record record = field.record();
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
                        field.setValue(record);
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
            } else if (clazz == Record.class) {
                value = field.record();
            } else {
                try {
                    value = convert(value, clazz);
                } catch (IllegalArgumentException e) {
                    throw new RuntimeException("Column " + column + ": " + e.getMessage());
                }
            }
        }

        return (T)value;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        boolean isFirst = true;

        if (isReadOnly()) {
            stringBuilder.append("read-only ");
        }

        stringBuilder.append("Row { ");

        for (Entry<String, Field> entry : fields.entrySet()) {
            if (isFirst) {
                isFirst = false;
            } else {
                stringBuilder.append(", ");
            }
            stringBuilder.append(entry.getKey());
            stringBuilder.append(" => ");
            stringBuilder.append(entry.getValue().rawValue());
        }
        stringBuilder.append(" }");

        return stringBuilder.toString();
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof Row) {
            return fields.equals(((Row)object).fields);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return fields.hashCode();
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
