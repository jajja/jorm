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

import com.jajja.jorm.Composite.Value;
import com.jajja.jorm.generator.Generator;

/**
 * <p>
 * Records provide the main interface for viewing and modifying data stored in
 * database tables. For generic SQL queries, examine {@link Transaction}
 * instead. For SQL syntax, examine {@link Query}
 * </p>
 * <p>
 * Records are not thread-safe! In fact, shared records will use thread-local
 * transactions with possibly unpredictable results for seemingly synchronized
 * execution.
 * </p>
 * <h3>Object relational mapping</h3>
 * <p>
 * Record template implementations can be automated according to JDBC types,
 * using the {@link Generator#string(String)} and
 * {@link Generator#string(String, String)} methods for targeted tables.
 * </p>
 * <p>
 * <strong>From SQL:</strong>
 *
 * <pre>
 * CREATE TABLE phrases (
 *     id        serial    NOT NULL,
 *     phrase    varchar   NOT NULL,
 *     locale_id integer   NOT NULL,
 *     PRIMARY KEY (id),
 *     UNIQUE (phrase, locale_id),
 *     FOREIGN KEY (locale_id) REFERENCES locales (id) ON DELETE CASCADE
 * )
 * </pre>
 *
 * </p>
 * <p>
 * <strong>To Java:</strong>
 *
 * <pre>
 * &#064;Table(database = &quot;default&quot;, table = &quot;phrases&quot;, id = &quot;id&quot;)
 * public class Phrase extends Record {
 *
 *     public Integer getId() {
 *         return get(&quot;id&quot;, Integer.class);
 *     }
 *
 *     public void setId(Integer id) {
 *         set(&quot;id&quot;, id);
 *     }
 *
 *     public String getPhrase() {
 *         return get(&quot;phrase&quot;, String.class);
 *     }
 *
 *     public void setPhrase(String phrase) {
 *         set(&quot;phrase&quot;, phrase);
 *     }
 *
 *     public Integer getLocaleId() {
 *         return get(&quot;locale_id&quot;, Integer.class);
 *     }
 *
 *     public void setLocaleId(Integer id) {
 *         set(&quot;locale_id&quot;, id);
 *     }
 *
 *     public Locale getLocale() {
 *         return get(&quot;locale_id&quot;, Locale.class);
 *     }
 *
 *     public void setLocale(Locale Locale) {
 *         set(&quot;locale_id&quot;, locale);
 *     }
 *
 * }
 * </pre>
 *
 * </p>
 * <p>
 * Note that related records are cached by the method
 * {@link Record#get(String, Class)}. Cache invalidation upon change of foreign
 * keys is maintained in records. Further control can be achieved by overriding
 * {@link Record#notifyFieldChanged(String, Object)}.
 * </p>
 *
 * @see Jorm
 * @see Query
 * @author Andreas Allerdahl &lt;andreas.allerdahl@jajja.com&gt;
 * @author Martin Korinth &lt;martin.korinth@jajja.com&gt;
 * @author Daniel Adolfsson &lt;daniel.adolfsson@jajja.com&gt;
 * @since 1.0.0
 */
public abstract class Record extends Row {
    public static enum ResultMode {
        /** For both INSERTs and UPDATEs, fully repopulate record(s) if supported. This is the default. */
        REPOPULATE,
        /** For INSERTs, fetch only generated keys. For UPDATEs, this is equivalent to NO_RESULT. */
        ID_ONLY,
        /** Fetch nothing. */
        NO_RESULT;
    }

    /**
     * Constructs a record. Uses {@link Jorm} annotation for configuration.
     */
    public Record() {
    }

    /**
     * Constructs a record, using the fields flags from a Row. Uses {@link Jorm} annotation for configuration.
     *
     * Note: no deep copying is done.
     */
    public Record(Row row) {
        this.fields = row.fields;
        this.flags = row.flags;
    }

    /**
     * Instantiates a record class of the specified type.
     *
     * @param clazz
     *            the @{link Record} class providing a new instance
     * @param <T> the @{link Record} class specification
     * @return the new instance
     */
    public static <T extends Record> T construct(Class<T> clazz) {
        try {
            return clazz.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate " + clazz, e);
        }
    }

    public Value id() {
        return get(primaryKey());
    }

    public Composite primaryKey() {
        return table().getPrimaryKey();
    }

    public static Composite primaryKey(Class<? extends Record> clazz) {
        return Table.get(clazz).getPrimaryKey();
    }

    /**
     * Provides the table mapping for the record.
     *
     * @return the table mapping.
     */
    public Table table() {
        return Table.get(getClass());
    }

    public boolean isPrimaryKeyNullOrChanged() {
        return isCompositeKeyNullOrChanged(primaryKey());
    }

    public boolean isPrimaryKeyNull() {
        return isCompositeKeyNull(primaryKey());
    }

    void assertPrimaryKeyNotNull() {
        if (isPrimaryKeyNull()) {
            throw new IllegalStateException("Primary key contains NULL value(s)");
        }
    }

    /**
     * Returns true if specified class is a subclass of Record.class.
     */
    public static boolean isRecordSubclass(Class<?> clazz) {
        return Record.class.isAssignableFrom(clazz) && !clazz.equals(Record.class);
    }

    /**
     * Marks all columns as changed.
     */
    @Override
    public void taint() {
        for (NamedField f : fields()) {
            if (!table().isImmutable(f.name()) && !primaryKey().contains(f.name())) {
                f.field().setChanged(true);
            }
        }
    }

    @Override
    public boolean isCompositeKeyNullOrChanged(Composite key) {
        return super.isCompositeKeyNullOrChanged(key);
    }

    @Override
    public boolean isCompositeKeyNull(Composite key) {
        return super.isCompositeKeyNull(key);
    }

    @Override
    public boolean isSet(String column) {
        return super.isSet(column);
    }

    @Override
    public void unset(String column) {
        assertNotReadOnly();
        super.unset(column);
    }

    @Override
    <T> T getColumnValue(String column, Class<T> clazz, boolean isReferenceCacheOnly, boolean throwSqlException, Transaction transaction) throws SQLException {
        return super.getColumnValue(column, clazz, isReferenceCacheOnly, throwSqlException, transaction);
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        Table table = table();

        if (table.getSchema() != null) {
            stringBuilder.append(table.getSchema());
            stringBuilder.append('.');
        }
        if (table.getTable() != null) {
            stringBuilder.append(table.getTable());
            stringBuilder.append(' ');
        }
        stringBuilder.append(super.toString());
        return stringBuilder.toString();
    }

    @Override
    public boolean equals(Object object) {
        if (getClass().isInstance(object)) {
            return id().equals(((Record)object).id());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return id().hashCode();
    }
}
