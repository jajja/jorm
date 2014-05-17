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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * {@link Record#notifyFieldChanged(Symbol, Object)}.
 * </p>
 *
 * @see Jorm
 * @see Query
 * @author Andreas Allerdahl <andreas.allerdahl@jajja.com>
 * @author Martin Korinth <martin.korinth@jajja.com>
 * @author Daniel Adolfsson <daniel.adolfsson@jajja.com>
 * @since 1.0.0
 */
public abstract class Record {
    public static final byte FLAG_STALE = 0x01;
    public static final byte FLAG_READ_ONLY = 0x02;
    Map<Symbol, Field> fields = new HashMap<Symbol, Field>(8, 1.0f);
    private Table table;
    private byte flags;
    private static Map<Class<? extends Record>, Logger> logs = new ConcurrentHashMap<Class<? extends Record>, Logger>(16, 0.75f, 1);

    public static enum ResultMode {
        /** For both INSERTs and UPDATEs, fully repopulate record(s). This is the default. */
        REPOPULATE,
        /** For INSERTs, fetch only generated keys, mark record(s) as stale. For UPDATEs, this is equivalent to NO_RESULT. */
        ID_ONLY,
        /** Fetch nothing, mark record as stale and assume the primary key value is accurate. */
        NO_RESULT;
    }

    public static class Field {
        private Object value = null;
        private boolean isChanged = false;
        private Record reference = null;

        private Field() {}

        void setValue(Object value) {
            this.value = value;
        }

        public Object getValue() {
            return value;
        }

        void setChanged(boolean isChanged) {
            this.isChanged = isChanged;
        }

        boolean isChanged() {
            return isChanged;
        }

        void setReference(Record reference) {
            this.reference = reference;
        }

        public Record getReference() {
            return reference;
        }
    }

    Field getOrCreateField(Symbol symbol) {
        Field field = fields.get(symbol);
        if (field == null) {
            field = new Field();
            fields.put(symbol, field);
        }
        return field;
    }

    /**
     * Provides a cached log for the specified class. Stores a new log in the
     * cache if no preceding call with the given class has been made.
     *
     * @param clazz
     *            the class defining log instance.
     * @return the class specific cached log.
     */
    public static Logger log(Class<? extends Record> clazz) {
        Logger log = logs.get(clazz);
        if (log == null) {
            synchronized (logs) {
                log = logs.get(clazz);
                if (log == null) {
                    log = LoggerFactory.getLogger(clazz);
                    logs.put(clazz, log);
                }
            }
        }
        return log;
    }

    /**
     * Provides the cached log for the instance class according to {@link Record#log(Class)}.
     */
    public Logger log() {
        return log(getClass());
    }

    // Helper method used by batch methods to quickly determine a list's generic type
    private static Class<? extends Record> genericType(Collection<? extends Record> records) {
        Iterator<? extends Record> iter = records.iterator();
        if (iter.hasNext()) {
            return iter.next().getClass();
        }
        throw new IllegalArgumentException("List is empty");
    }


    /**
     * Constructs a mapped record. Depends on {@link Jorm} annotation for table
     * mapping.
     */
    public Record() {
        table = Table.get(getClass());
    }

    /**
     * Constructs a mapped record. Mainly intended for anonymous record
     * instantiation such as the results from the transaction select methods
     * {@link Transaction#select(Query)},
     * {@link Transaction#select(String, Object...)},
     * {@link Transaction#selectAll(Query)} and
     * {@link Transaction#selectAll(String, Object...)}.
     *
     * @param table
     *            the table mapping.
     */
    public Record(Table table) {
        this.table = table;
    }

    /**
     * Instantiates a record class of the specified type.
     */
    public static <T extends Record> T construct(Class<T> clazz) {
        try {
            return clazz.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate " + clazz, e);
        }
    }

    /**
     * Notifies field changes. The default implementation is empty, but provides
     * the option to override and act upon changes. This method is called
     * whenever {@link #set(String, Object)} or {@link #set(Symbol, Object)}
     * changes a field, or {@link #populate(ResultSet)} is called.
     *
     * @param symbol
     *            the symbol of the column.
     * @param object
     *            the value of the field after change.
     */
    protected void notifyFieldChanged(Symbol symbol, Object object) { }

    public Value id() {
        return get(primaryKey());
    }

    public Composite primaryKey() {
        return table.getPrimaryKey();
    }

    public static Composite primaryKey(Class<? extends Record> clazz) {
        return Table.get(clazz).getPrimaryKey();
    }

    public Value get(Composite composite) {
        return composite.valueFrom(this);
    }

    /**
     * Provides the table mapping for the record.
     *
     * @return the table mapping.
     */
    public Table table() {
        return table;
    }

    /**
     * Provides an immutable view of the fields of the record.
     *
     * @return the fields.
     */
    public Map<Symbol, Field> fields() {
        return Collections.unmodifiableMap(fields);
    }

    /**
     * <p>
     * Deprecated: Use {@link #transaction(Class)}
     *
     * Opens a thread local transaction to the database mapped by the record
     * class. If an open transaction already exists for the record class, it is
     * reused. This method is idempotent when called from the same thread.
     * </p>
     * <p>
     * This is corresponds to a call to {@link Database#open(String)} for the
     * database named by the class mapping of the record. Requires the given
     * class to be mapped by {@link Jorm}.
     * </p>
     *
     * @param clazz
     *            the mapped record class.
     * @return the open transaction.
     */
    @Deprecated
    public static Transaction open(Class<? extends Record> clazz) {
        return transaction(clazz);
    }

    /**
     * <p>
     * Opens a thread local transaction to the database mapped by the record
     * class. If an open transaction already exists for the record class, it is
     * reused. This method is idempotent when called from the same thread.
     * </p>
     * <p>
     * This is corresponds to a call to {@link Database#open(String)} for the
     * database named by the class mapping of the record. Requires the given
     * class to be mapped by {@link Jorm}.
     * </p>
     *
     * @param clazz
     *            the mapped record class.
     * @return the transaction.
     */
    public static Transaction transaction(Class<? extends Record> clazz) {
        return Database.open(Table.get(clazz).getDatabase());
    }

    /**
     * <p>
     * Deprecated: Use transaction(Class).commit();
     *
     * Commits the thread local transaction to the named database mapped by the
     * record class, if it has been opened.
     * </p>
     * <p>
     * This is corresponds to a call to {@link Database#commit(String)} for the
     * database named by the class mapping of the record. Requires the given
     * class to be mapped by {@link Jorm}.
     * </p>
     *
     * @param clazz
     *            the mapped record class.
     * @return the committed transaction or null for no active transaction.
     */
    @Deprecated
    public static Transaction commit(Class<? extends Record> clazz) throws SQLException {
        return Database.commit(Table.get(clazz).getDatabase());
    }

    /**
     * <p>
     * Deprecated: Use transaction(Class).close();
     *
     * Closes the thread local transaction to the named database mapped by the
     * record class, if it has been opened. This method is idempotent when
     * called from the same thread.
     * </p>
     * <p>
     * This is corresponds to a call to {@link Database#close(String)} for the
     * database named by the class mapping of the record. Requires the given
     * class to be mapped by {@link Jorm}.
     * </p>
     *
     * @param clazz
     *            the mapped record class.
     * @return the closed transaction or null for no active transaction.
     */
    @Deprecated
    public static Transaction close(Class<? extends Record> clazz) {
        return Database.close(Table.get(clazz).getDatabase());
    }

    /**
     * <p>
     * Deprecated: Use {@link #transaction()}
     *
     * Opens a thread local transaction to the named database mapped by the
     * record. If an open transaction already exists for the record, it is
     * reused. This method is idempotent when called from the same thread.
     * </p>
     * <p>
     * This is corresponds to a call to {@link Database#open(String)} for the
     * database named by the table mapping of the record.
     * </p>
     *
     * @return the open transaction.
     */
    @Deprecated
    public Transaction open() {
        return Database.open(table.getDatabase());
    }

    /**
     * <p>
     * Opens a thread local transaction to the named database mapped by the
     * record. If an open transaction already exists for the record, it is
     * reused. This method is idempotent when called from the same thread.
     * </p>
     * <p>
     * This is corresponds to a call to {@link Database#open(String)} for the
     * database named by the table mapping of the record.
     * </p>
     *
     * @return the transaction.
     */
    public Transaction transaction() {
        return Database.open(table.getDatabase());
    }

    /**
     * <p>
     * Deprecated: Use transaction().commit()
     *
     * Commits the thread local transaction to the named database mapped by the
     * record, if it has been opened.
     * </p>
     * <p>
     * This is corresponds to a call to {@link Database#commit(String)} for the
     * database named by the table mapping of the record.
     * </p>
     * <p>
     * <strong>Note:</strong> This may cause changes of other records to be
     * persisted to the mapped database of the record, since all records mapped
     * to the same named database share transaction in the context of the
     * current thread.
     * </p>
     *
     * @throws SQLException
     *             if a database access error occurs.
     * @return the committed transaction or null for no active transaction.
     */
    @Deprecated
    public Transaction commit() throws SQLException {
        return Database.commit(table.getDatabase());
    }

    /**
     * <p>
     * Deprecated: Use transaction().close()
     *
     * Closes the thread local transaction to the named database mapped by the
     * record, if it has been opened. This method is idempotent when called from
     * the same thread.
     * </p>
     * <p>
     * This is corresponds to a call to {@link Database#close(String)} for the
     * database named by the table mapping of the record.
     * </p>
     * <p>
     * <strong>Note:</strong> This may cause changes of other records to be
     * discarded in the mapped database of the record, since all records mapped
     * to the same named database share transaction in the context of the
     * current thread.
     * </p>
     *
     * @return the closed transaction or null for no active transaction.
     */
    @Deprecated
    public Transaction close() {
        return Database.close(table.getDatabase());
    }

    /**
     * Populates the record with the first result for which the given column name
     * matches the given value.
     *
     * @param symbol
     *            the column symbol.
     * @param value
     *            the value to match.
     * @return true if the record could be updated with a matching row from the
     *         table.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public boolean populateByComposite(Composite composite, Value value) throws SQLException {
        return transaction().populateByComposite(this, composite, value);
    }

    /**
     * Populates the record with the result for which the id column matches the
     * given value.
     *
     * @param id
     *            the id value to match.
     * @return true if the record could be updated with a matching row from the
     *         table.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public boolean populateById(Object id) throws SQLException {
        return transaction().populateById(this, id);
    }

    /**
     * Builds a generic SQL query for the record.
     *
     * @param sql
     *            the SQL statement to represent the query.
     * @return the built query.
     */
    public Query build(String sql) {
        return transaction().build(sql);
    }

    /**
     * Builds a generic SQL query for the record and quotes identifiers from the
     * given parameters according to the SQL dialect of the mapped database of
     * the record.
     *
     * @param sql
     *            the Jorm SQL statement to represent the query.
     * @param params
     *            the parameters applying to the SQL hash markup.
     * @return the built query.
     */
    public Query build(String sql, Object... params) {
        return transaction().build(sql, params);
    }

    /**
     * Builds a generic SQL query for a given record class.
     *
     * @param clazz
     *            the mapped record class.
     * @param sql
     *            the SQL statement to represent the query.
     * @return the built query.
     */
    public static Query build(Class<? extends Record> clazz, String sql) {
        return transaction(clazz).build(sql);
    }

    /**
     * Builds a generic SQL query for a given record class and quotes
     * identifiers from the given parameters according to the SQL dialect of the
     * mapped database of the record class.
     *
     * @param clazz
     *            the mapped record class.
     * @param sql
     *            the Jorm SQL statement to represent the query.
     * @param params
     *            the parameters applying to the SQL hash markup.
     * @return the built query.
     * @return the built query.
     */
    public static Query build(Class<? extends Record> clazz, String sql, Object... params) {
        return transaction(clazz).build(sql, params);
    }

    /**
     * Provides a selected record from the mapped database table, populated with
     * the first result for which the composite key matches.
     *
     * @param clazz
     *            the class defining the table mapping.
     * @param composite
     *            the composite key
     * @param value
     *            the composite key value
     * @return the matched record or null for no match.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static <T extends Record> T find(Class<T> clazz, Composite composite, Object value) throws SQLException {
        return transaction(clazz).find(clazz, composite, value);
    }

    /**
     * Provides a selected record from the mapped database table, populated with
     * the first result for which the simple key matches.
     *
     * @param clazz
     *            the class defining the table mapping.
     * @param composite
     *            simple key
     * @param value
     *            the composite key value
     * @return the matched record or null for no match.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static <T extends Record> T find(Class<T> clazz, String column, Object value) throws SQLException {
        return transaction(clazz).find(clazz, column, value);
    }

    /**
     * Provides a complete list of selected records from the mapped database
     * table, populated with the results for which the composite key matches.
     *
     * @param clazz
     *            the class defining the table mapping.
     * @param composite
     *            the composite key
     * @param value
     *            the composite key value
     * @return the matched records.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static <T extends Record> List<T> findAll(Class<T> clazz, Composite composite, Value value) throws SQLException {
        return transaction(clazz).findAll(clazz, composite, value);
    }

    /**
     * Provides a complete list of selected records from the mapped database
     * table, populated with the results for which the simple key matches.
     *
     * @param clazz
     *            the class defining the table mapping.
     * @param composite
     *            the composite key
     * @param value
     *            the composite key value
     * @return the matched records.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static <T extends Record> List<T> findAll(Class<T> clazz, String column, Object value) throws SQLException {
        return transaction(clazz).findAll(clazz, column, value);
    }

    public static <T extends Record> List<T> findAll(Class<T> clazz) throws SQLException {
        return transaction(clazz).findAll(clazz);
    }

    public static RecordIterator iterate(Class<? extends Record> clazz, Composite composite, Value value) throws SQLException {
        return transaction(clazz).iterate(clazz, composite, value);
    }

    public static RecordIterator iterate(Class<? extends Record> clazz) throws SQLException {
        return transaction(clazz).iterate(clazz);
    }

    /**
     * Provides a selected record, populated with the result for which the primary key
     * column matches the given id value.
     *
     * @param clazz
     *            the class defining the table mapping.
     * @param id
     *            the primary key value (can be either a {@link Composite.Value} or a single column value).
     * @return the matched record or null for no match.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static <T extends Record> T findById(Class<T> clazz, Object id) throws SQLException {
        return transaction(clazz).findById(clazz, id);
    }

    /**
     * Provides a selected record, populated with the first result from the
     * query given by a plain SQL statement and applicable parameters.
     *
     * @param clazz
     *            the class defining the table mapping.
     * @param sql
     *            the plain SQL statement.
     * @return the matched record or null for no match.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static <T extends Record> T select(Class<T> clazz, String sql) throws SQLException {
        return transaction(clazz).select(clazz, sql);
    }

    /**
     * Provides a selected record, populated with the first result from the
     * query given by a Jorm SQL statement and applicable parameters.
     *
     * @param clazz
     *            the class defining the table mapping.
     * @param sql
     *            the Jorm SQL statement.
     * @param params
     *            the applicable parameters.
     * @return the matched record or null for no match.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static <T extends Record> T select(Class<T> clazz, String sql, Object... params) throws SQLException {
        return transaction(clazz).select(clazz, sql, params);
    }

    /**
     * Provides a selected record, populated with the first result from the
     * given query.
     *
     * @param clazz
     *            the class defining the table mapping.
     * @param query
     *            the query.
     * @return the matched record or null for no match.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static <T extends Record> T select(Class<T> clazz, Query query) throws SQLException {
        return transaction(clazz).select(clazz, query);
    }

    /**
     * Provides a list of selected records, populated with the results from the
     * query given by a plain SQL statement.
     *
     * @param clazz
     *            the class defining the table mapping.
     * @param sql
     *            the plain SQL statement.
     * @return the matched records
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static <T extends Record> List<T> selectAll(Class<T> clazz, String sql) throws SQLException {
        return transaction(clazz).selectAll(clazz, sql);
    }

    /**
     * Provides a list of selected records, populated with the results from the
     * query given by a Jorm SQL statement and applicable parameters.
     *
     * @param clazz
     *            the class defining the table mapping.
     * @param sql
     *            the Jorm SQL statement.
     * @param params
     *            the applicable parameters.
     * @return the matched records
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static <T extends Record> List<T> selectAll(Class<T> clazz, String sql, Object... params) throws SQLException {
        return transaction(clazz).selectAll(clazz, sql, params);
    }

    /**
     * Provides a list of records populated with the results from the given query.
     *
     * @param clazz
     *            the class defining the table mapping.
     * @param query
     *            the query.
     * @return the matched records.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static <T extends Record> List<T> selectAll(Class<T> clazz, Query query) throws SQLException {
        return transaction(clazz).selectAll(clazz, query);
    }

    /**
     * Provides a record iterator with the results from the query given by
     * a plain SQL statement.
     *
     * @param clazz
     *            the class defining the table mapping.
     * @param sql
     *            the plain SQL statement.
     * @return the matched records
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static RecordIterator selectIterator(Class<? extends Record> clazz, String sql) throws SQLException {
        return transaction(clazz).selectIterator(clazz, sql);
    }

    /**
     * Provides a record iterator with the results from the query given by
     * a Jorm SQL statement and applicable parameters.
     *
     * @param clazz
     *            the class defining the table mapping.
     * @param sql
     *            the Jorm SQL statement.
     * @param params
     *            the applicable parameters.
     * @return the matched records
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static RecordIterator selectIterator(Class<? extends Record> clazz, String sql, Object... params) throws SQLException {
        return transaction(clazz).selectIterator(clazz, sql, params);
    }

    /**
     * Provides a record iterator with the results from the given query.
     *
     * @param clazz
     *            the class defining the table mapping.
     * @param query
     *            the query.
     * @return the matched records.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static RecordIterator selectIterator(Class<? extends Record> clazz, Query query) throws SQLException {
        return transaction(clazz).selectIterator(clazz, query);
    }

    /**
     * Provides a hash map of selected records, populated with the results from the
     * given query.
     *
     * @param clazz
     *            the class defining the table mapping.
     * @param column
     *            the column to use as key.
     * @param query
     *            the query.
     * @return the matched records.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static <T extends Record> Map<Composite.Value, T> selectAsMap(Class<T> clazz, Composite compositeKey, boolean allowDuplicates, Query query) throws SQLException {
        return transaction(clazz).selectAsMap(clazz, compositeKey, allowDuplicates, query);
    }

    public static <T extends Record> Map<Composite.Value, T> selectAsMap(Class<T> clazz, Composite compositeKey, boolean allowDuplicates, String sql, Object... params) throws SQLException {
        return transaction(clazz).selectAsMap(clazz, compositeKey, allowDuplicates, sql, params);
    }

    public static <T extends Record> Map<Composite.Value, T> selectAsMap(Class<T> clazz, Composite compositeKey, boolean allowDuplicates) throws SQLException {
        return transaction(clazz).selectAsMap(clazz, compositeKey, allowDuplicates);
    }

    /**
     * Provides a hash map of selected records, populated with the results from the
     * given query.
     *
     * @param clazz
     *            the class defining the table mapping.
     * @param column
     *            the column to use as key.
     * @param query
     *            the query.
     * @return the matched records.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static <T extends Record> Map<Composite.Value, List<T>> selectAllAsMap(Class<T> clazz, Composite compositeKey, Query query) throws SQLException {
        return transaction(clazz).selectAllAsMap(clazz, compositeKey, query);
    }

    public static <T extends Record> Map<Composite.Value, List<T>> selectAllAsMap(Class<T> clazz, Composite compositeKey, String sql, Object... params) throws SQLException {
        return transaction(clazz).selectAllAsMap(clazz, compositeKey, sql, params);
    }

    public static <T extends Record> Map<Composite.Value, List<T>> selectAllAsMap(Class<T> clazz, Composite compositeKey) throws SQLException {
        return transaction(clazz).selectAllAsMap(clazz, compositeKey);
    }

    /**
     * Executes the query given by a plain SQL statement and applicable
     * parameters and populates the record with the first row of the result. Any
     * values in the record object are cleared if the record was previously
     * populated.
     *
     * @param sql
     *            the plain SQL statement.
     * @return true if the record was populated, otherwise false.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public boolean selectInto(String sql) throws SQLException {
        return transaction().selectInto(this, sql);
    }

    /**
     * Executes the query given by a Jorm SQL statement and applicable
     * parameters and populates the record with the first row of the result. Any
     * values in the record object are cleared if the record was previously
     * populated.
     *
     * @param sql
     *            the Jorm SQL statement.
     * @param params
     *            the applicable parameters.
     * @return true if the record was populated, otherwise false.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public boolean selectInto(String sql, Object... params) throws SQLException {
        return transaction().selectInto(this, sql, params);
    }

    /**
     * Executes the given query and populates the record with the first row of
     * the result. Any values in the record object are cleared if the record was
     * previously populated.
     *
     * @param query
     *            the query.
     * @return true if the record was populated, otherwise false.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public boolean selectInto(Query query) throws SQLException {
        return transaction().selectInto(this, query);
    }

    /**
     * Populates all records in the given collection of records with a single
     * prefetched reference of the given record class. Existing cached
     * references are not overwritten.
     *
     * @param records
     *            the records to populate with prefetched references.
     * @param foreignKeySymbol
     *            the symbol defining the foreign key to the referenced records.
     * @param clazz
     *            the class of the referenced records.
     * @param referredSymbol
     *            the symbol defining the referred column of the referenced
     *            records.
     * @return the prefetched records.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static <T extends Record> Map<Composite.Value, T> prefetch(Collection<? extends Record> records, Symbol foreignKeySymbol, Class<T> clazz, Symbol referredSymbol) throws SQLException {
        if (!records.isEmpty()) {
            return transaction(genericType(records)).prefetch(records, foreignKeySymbol, clazz, referredSymbol);
        }
        return new HashMap<Composite.Value, T>();
    }

    public static <T extends Record> Map<Composite.Value, T> prefetch(Collection<? extends Record> records, Symbol foreignKeySymbol, Class<T> clazz, Symbol referredSymbol, boolean ignoreInvalidReferences) throws SQLException {
        if (!records.isEmpty()) {
            return transaction(genericType(records)).prefetch(records, foreignKeySymbol, clazz, referredSymbol, ignoreInvalidReferences);
        }
        return new HashMap<Composite.Value, T>();
    }

    /**
     * Populates all records in the given collection of records with a single
     * prefetched reference of the given record class. Existing cached
     * references are not overwritten.
     *
     * @param records
     *            the records to populate with prefetched references.
     * @param foreignKeySymbol
     *            the column name defining the foreign key to the referenced records.
     * @param clazz
     *            the class of the referenced records.
     * @param referredSymbol
     *            the column name defining the referred column of the referenced
     *            records.
     * @return the prefetched records.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static <T extends Record> Map<Composite.Value, T> prefetch(Collection<? extends Record> records, String foreignKeySymbol, Class<T> clazz, String referredSymbol) throws SQLException {
        if (!records.isEmpty()) {
            return transaction(genericType(records)).prefetch(records, foreignKeySymbol, clazz, referredSymbol);
        }
        return new HashMap<Composite.Value, T>();
    }

    public static <T extends Record> Map<Composite.Value, T> prefetch(Collection<? extends Record> records, String foreignKeySymbol, Class<T> clazz, String referredSymbol, boolean ignoreInvalidReferences) throws SQLException {
        if (!records.isEmpty()) {
            return transaction(genericType(records)).prefetch(records, foreignKeySymbol, clazz, referredSymbol, ignoreInvalidReferences);
        }
        return new HashMap<Composite.Value, T>();
    }

    /**
     * Populates the record with the first row of the result. Any values in the
     * record object are cleared if the record was previously populated. Returns
     * true if the record was populated, false otherwise (no rows in resultSet).
     *
     * @return true if populated, false otherwise.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public boolean populate(ResultSet resultSet) throws SQLException {
        try {
            RecordIterator iter = null;
            try {
                iter = new RecordIterator(resultSet);
                if (iter.next()) {
                    iter.record(this);
                    return true;
                }
            } finally {
                 if (iter != null) iter.close();
            }
        } catch (SQLException e) {
            throw transaction().getDialect().rethrow(e);
        }
        return false;
    }

    boolean isPrimaryKeyNullOrChanged() {
        for (Symbol symbol : primaryKey().getSymbols()) {
            Field field = fields.get(symbol);
            if (field == null || field.getValue() == null || field.isChanged()) {
                return true;
            }
        }
        return false;
    }

    boolean isPrimaryKeyNull() {
        for (Symbol symbol : primaryKey().getSymbols()) {
            Field field = fields.get(symbol);
            if (field == null || field.getValue() == null) {
                return true;
            }
        }
        return false;
    }

    void assertPrimaryKeyNotNull() {
        if (isPrimaryKeyNull()) {
            throw new IllegalStateException("Primary key contains NULL value(s)");
        }
    }

    /**
     * Save the record. This is done by a call to {@link #insert()} if the id
     * field is null, unset or changed, otherwise by a call to {@link #update()}.
     *
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public void save(ResultMode mode) throws SQLException {
        transaction().save(this, mode);
    }

    public void save() throws SQLException {
        transaction().save(this);
    }

    /**
     * Batch saves the records. This is done by a call to {@link #insert()} if the id
     * field is null, unset or changed, otherwise by a call to {@link #update()}.
     *
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static void save(Collection<? extends Record> records, int batchSize, ResultMode mode) throws SQLException {
        if (!records.isEmpty()) {
            transaction(genericType(records)).save(records, batchSize, mode);
        }
    }

    /**
     * Batch saves the records. This is done by a call to {@link #insert()} if the id
     * field is null, unset or changed, otherwise by a call to {@link #update()}.
     *
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static void save(Collection<? extends Record> records) throws SQLException {
        if (!records.isEmpty()) {
            transaction(genericType(records)).save(records, 0, ResultMode.REPOPULATE);
        }
    }

    /**
     * Deletes the record row from the database by executing the SQL query "DELETE FROM [tableName] WHERE [primaryKey] = [primaryKeyColumnValue]".
     * The primary key column value is also set to null.
     *
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public void delete() throws SQLException {
        transaction().delete(this);
    }

    /**
     * Deletes multiple records by exeuting a DELETE FROM table WHERE id IN (...)
     *
     * @param records List of records to delete (must be of the same class, and bound to the same Database)
     * @throws SQLException
     *             if a database access error occurs.
     */
    public static void delete(Collection<? extends Record> records) throws SQLException {
        if (!records.isEmpty()) {
            transaction(genericType(records)).delete(records);
        }
    }

    /**
     * Marks all fields as changed.
     */
    public void taint() {
        for (Entry<Symbol, Field> entry : fields.entrySet()) {
            Symbol symbol = entry.getKey();
            Field field = entry.getValue();
            if (!table.isImmutable(symbol) && !primaryKey().contains(symbol)) {
                field.setChanged(true);
            }
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

    /**
     * Determines whether the record has been changed or not.
     *
     * @return true if at least one field has been changed, otherwise false.
     */
    public boolean isChanged() {
        for (Field field : fields.values()) {
            if (field.isChanged()) {
                return true;
            }
        }
        return false;
    }

    private void flag(int flag, boolean set) {
        if (set) {
            flags |= flag;
        } else {
            flags &= ~flag;
        }
    }

    private boolean flag(int flag) {
        return (flags &= flag) != 0;
    }

    /**
     * Deprecated: use stale(true)
     * Marks this record as stale. It will be re-populated on the next call to
     * {@link #set(String, Object)}, {@link #set(Symbol, Object)},
     * {@link #get(String)}, {@link #get(Symbol)} or {@link #refresh()},
     * whichever comes first.
     */
    @Deprecated
    public void markStale() {
        flag(FLAG_STALE, true);
    }

    /**
     * Marks this record as stale or fresh. Stale records are re-populated
     * with fresh values from the database on the next call to
     * {@link #set(String, Object)}, {@link #set(Symbol, Object)},
     * {@link #get(String)}, {@link #get(Symbol)} or {@link #refresh()},
     * whichever comes first.
     */
    public void stale(boolean setStale) {
        flag(FLAG_STALE, setStale);
    }

    /**
     * Determines whether the record is stale or not, i.e. needs to be
     * re-populated in any upcoming call to {@link #set(String, Object)},
     * {@link #set(Symbol, Object)}, {@link #get(String)}, {@link #get(Symbol)}
     * or {@link #refresh()}, whichever comes first.
     *
     * @return true if the record is stale otherwise false.
     */
    public boolean isStale() {
        return flag(FLAG_STALE);
    }

    /**
     * Sets the record as read only according to given value
     *
     * @param setReadOnly
     *            the value determining read only state of the record.
     * @throws RuntimeException
     *             whenever a record is set to read only without table mapping
     *             provided by an {@link Jorm} annotation, i.e. on anonymous
     *             records retrieved through calls to
     *             {@link Transaction#select(Query)},
     *             {@link Transaction#select(String, Object...)},
     *             {@link Transaction#selectAll(Query)} and
     *             {@link Transaction#selectAll(String, Object...)}.
     */
    public void readOnly(boolean setReadOnly) {
        if (primaryKey() == null && setReadOnly) {
            throw new RuntimeException("Cannot mark anonymous records as read only!");
        }
        flag(FLAG_READ_ONLY, setReadOnly);
    }

    /**
     * Returns true if this record is read only.
     */
    boolean isReadOnly() {
        return flag(FLAG_READ_ONLY);
    }

    void ensureNotReadOnly() {
        if (isReadOnly()) {
            throw new RuntimeException("Record is read only!");
        }
    }

    /**
     * Inserts the record's changed values into the database by executing an SQL INSERT query.
     * The record's primary key value is set to the primary key generated by the database.
     *
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public void insert(ResultMode mode) throws SQLException {
        transaction().insert(this, mode);
    }

    public void insert() throws SQLException {
        transaction().insert(this);
    }

    /**
     * Executes a batch INSERT (INSERT INTO ... (columns...) VALUES (row1), (row2), (row3), ...) and repopulates the list with stored entities.
     *
     * @param records List of records to insert (must be of the same class, and bound to the same Database)
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static void insert(Collection<? extends Record> records, ResultMode mode) throws SQLException {
        if (!records.isEmpty()) {
            transaction(genericType(records)).insert(records, mode);
        }
    }

    public static void insert(Collection<? extends Record> records) throws SQLException {
        if (!records.isEmpty()) {
            transaction(genericType(records)).insert(records);
        }
    }

    /**
     * Executes a batch INSERT (INSERT INTO ... (columns...) VALUES (row1), (row2), (row3), ...).
     *
     * For large sets of records, the use of chunkSize is recommended to avoid out-of-memory errors and too long SQL queries.
     *
     * Setting isFullRepopulate to true will re-populate the record fields with fresh values. This will generate
     * an additional SELECT query for every chunk of records for databases that do not support RETURNING.
     *
     * @param records List of records to insert (must be of the same class, and bound to the same Database)
     * @param chunkSize Splits the records into chunks, <= 0 disables
     * @param isFullRepopulate Whether or not to fully re-populate the record fields, or just update their primary key value and markStale()
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static void insert(Collection<? extends Record> records, int chunkSize, ResultMode mode) throws SQLException {
        if (!records.isEmpty()) {
            transaction(genericType(records)).insert(records, chunkSize, mode);
        }
    }

    /**
     * Updates the record's changed column values by executing an SQL UPDATE query.
     *
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public void update(ResultMode mode) throws SQLException {
        transaction().update(this, mode);
    }

    public void update() throws SQLException {
        transaction().update(this);
    }

    /**
     * Executes a batch UPDATE (UPDATE ... SET x = s.x, y = s.y FROM (values, ...) s WHERE id = s.id).
     *
     * Currently, this is only supported on PostgreSQL. The method will fall back to using individual update()s on other databases.
     *
     * @param records List of records to insert (must be of the same class, and bound to the same Database)
     * @throws SQLException
     *             if a database access error occurs
     */
    public static void update(Collection<? extends Record> records) throws SQLException {
        if (!records.isEmpty()) {
            transaction(genericType(records)).update(records);
        }
    }

    /**
     * Executes a batch UPDATE (UPDATE ... SET x = s.x, y = s.y FROM (values, ...) s WHERE id = s.id).
     *
     * For large sets of records, the use of chunkSize is recommended to avoid out-of-memory errors and too long SQL queries.
     *
     * Currently, this is only supported on PostgreSQL. The method will fall back to using individual update()s on other databases.
     *
     * @param records List of records to insert (must be of the same class, and bound to the same Database)
     * @param chunkSize Splits the records into chunks, <= 0 disables
     * @param mode
     * @throws SQLException
     *             if a database access error occurs
     */
    public static void update(Collection<? extends Record> records, int chunkSize, ResultMode mode) throws SQLException {
        if (!records.isEmpty()) {
            transaction(genericType(records)).update(records, chunkSize, mode);
        }
    }

    /**
     * Executes a batch UPDATE (UPDATE ... SET x = s.x, y = s.y FROM (values, ...) s WHERE id = s.id).
     *
     * For large sets of records, the use of chunkSize is recommended to avoid out-of-memory errors and too long SQL queries.
     *
     * Currently, this is only supported on PostgreSQL. The method will fall back to using individual update()s on other databases.
     *
     * @param records List of records to insert (must be of the same class, and bound to the same Database)
     * @param chunkSize Splits the records into chunks, <= 0 disables
     * @param mode
     * @param primaryKey
     * @throws SQLException
     *             if a database access error occurs
     */
    public static void update(Collection<? extends Record> records, int chunkSize, ResultMode mode, Composite primaryKey) throws SQLException {
        if (!records.isEmpty()) {
            transaction(genericType(records)).update(records, chunkSize, mode, primaryKey);
        }
    }

    /**
     * Determines whether a field has been changed or not.
     *
     * @param symbol
     *            the symbol of the column name defining the field.
     * @return true if the field has been changed, false otherwise.
     */
    public boolean isFieldChanged(Symbol symbol) {
        Field field = fields.get(symbol);
        if (field == null) {
            return false;
        }
        return field.isChanged();
    }

    /**
     * Returns true if specified class is a subclass of Record.class.
     */
    public static boolean isRecordSubclass(Class<?> clazz) {
        return Record.class.isAssignableFrom(clazz) && !clazz.equals(Record.class);
    }

    /**
     * Re-populates a stale record with fresh database values by a select query.
     * A record is considered stale after a call to either
     * {@link Record#insert()} or {@link Record#insert()}, if the SQL dialect of
     * the mapped database does not support returning. A record mapped to a
     * table in a Postgres database is thus never stale.
     *
     *
     * @throws RuntimeException
     *             whenever a SQLException occurs.
     */
    public void refresh() {
        if (isStale()) {
            try {
                assertPrimaryKeyNotNull();
                populateById(primaryKey().valueFrom(this, true));
            } catch (SQLException e) {
                throw new RuntimeException("Failed to refresh stale record", e);
            }
            stale(false);
        }
    }

    private boolean isChanged(Symbol symbol, Object newValue) {
        if (isReadOnly() || table.isImmutable(symbol)) {
            return false;
        }

        Field field = fields.get(symbol);
        if (field == null) {
            return true;
        }

        Object oldValue = field.getValue();
        if (oldValue == null && newValue == null) {
            return false;
        } else {
            return oldValue == null || !oldValue.equals(newValue);
        }
    }

    void put(Symbol symbol, Object value) {
        refresh();

        boolean isChanged;
        Field field = fields.get(symbol);
        if (field == null) {
            field = new Field();
        }

        if (value != null && isRecordSubclass(value.getClass())) {
            Record record = (Record)value;
            if (!record.primaryKey().isSingle()) {
                throw new UnsupportedOperationException("Composite foreign key references are not supported");
            }
            Object id = record.id().getValue();
            if (id == null) {
                throw new NullPointerException("While setting " + record + "." + symbol.getName() + " = " + value + " -- id (primary key) is null -- perhaps you need to save()?");
            }
            isChanged = isChanged(symbol, id);
            if (isChanged) {
                notifyFieldChanged(symbol, value);
            }
            field.setReference(record);
            field.setValue(id);
        } else {
            isChanged = isChanged(symbol, value);
            if (isChanged) {
                notifyFieldChanged(symbol, value);
            }
            if (isChanged) {
                field.setReference(null); // invalidate cached reference
            }
            field.setValue(value);
        }

        if (isChanged) {
            // it's OK to mark the id column as changed here
            field.setChanged(true);
        }

        fields.put(symbol, field);
    }

    /**
     * Sets the specified field corresponding to a column of the mapped record.
     * Any field values extending {@link Record} are cached until the field is
     * changed again, and the mapped id of the record is set as field value
     * instead.
     *
     * @param column
     *            the name of the column corresponding to the field to set.
     * @param value
     *            the value.
     */
    public void set(String column, Object value) {
        set(Symbol.get(column), value);
    }

    /**
     * Sets the specified field corresponding to a column of the mapped record.
     * Any field values extending {@link Record} are cached until the field is
     * changed again, and the mapped id of the record is set as field value
     * instead.
     *
     * @param symbol
     *            the symbol of the column corresponding to the field to set.
     * @param value
     *            the value.
     */
    public void set(Symbol symbol, Object value) {
        ensureNotReadOnly();
        put(symbol, value);
    }

    /**
     * Unsets the specified field corresponding to a column of the mapped record.
     *
     * @param column
     *            the name of the column corresponding to the field to set.
     */
    public void unset(String column) {
        unset(Symbol.get(column));
    }

    /**
     * Unsets the specified field corresponding to a column of the mapped record.
     *
     * @param symbol
     *            the symbol of the column corresponding to the field to set.
     */
    public void unset(Symbol symbol) {
        ensureNotReadOnly();
        Field field;

        refresh();

        field = fields.get(symbol);
        if (field != null) {
            notifyFieldChanged(symbol, null);
            fields.remove(symbol);
        }
    }

    /**
     * Determines whether the field corresponding to a given column name is set
     * or not.
     *
     * @param column
     *            the name of the column corresponding to the field to set.
     * @return true if the field is set, false otherwise.
     */
    public boolean isSet(String column) {
        return isSet(Symbol.get(column));
    }

    /**
     * Determines whether the field corresponding to a given column name is set
     * or not.
     *
     * @param symbol
     *            the symbol of the column corresponding to the field to set.
     * @return true if the field is set, false otherwise.
     */
    public boolean isSet(Symbol symbol) {
        refresh();
        return fields.get(symbol) != null;
    }

    /**
     * Provides a cached instance of a record represented by a field defined by
     * a given column name. If the record has not previously been cached it is
     * fetched from the database and cached.
     *
     * @param column
     *            the column name.
     * @param clazz
     *            the expected class of the cached record.
     * @return the cached record corresponding to the given symbol.
     */
    public <T> T get(String column, Class<T> clazz) {
        try {
            return getField(Symbol.get(column), clazz, false, false);
        } catch (SQLException e) {
            // UNREACHABLE
            throw new IllegalStateException(e);
        }
    }

    public <T extends Record> T ref(String column, Class<T> clazz) throws SQLException {
        return getField(Symbol.get(column), clazz, false, true);
    }

    /**
     * Provides a cached instance of a record represented by a field defined by
     * a given symbol for a column name. If the record has not previously been
     * cached it is fetched from the database and cached.
     *
     * @param symbol
     *            the symbol defining the column name.
     * @param clazz
     *            the expected class of the cached record.
     * @return the cached record corresponding to the given symbol.
     */
    public <T> T get(Symbol symbol, Class<T> clazz) {
        try {
            return getField(symbol, clazz, false, false);
        } catch (SQLException e) {
            // UNREACHABLE
            throw new IllegalStateException(e);
        }
    }

    public <T extends Record> T ref(Symbol symbol, Class<T> clazz) throws SQLException {
        return getField(symbol, clazz, false, true);
    }

    /**
     * Provides a cached instance of a record represented by a field defined by
     * a given symbol for a column name.
     *
     * @param symbol
     *            the symbol defining the column name.
     * @param clazz
     *            the expected class of the cached record.
     * @param isCacheOnly only retrieves previously cached values.
     * @return the cached record corresponding to the given symbol.
     */
    public <T extends Record> T get(Symbol symbol, Class<T> clazz, boolean isCacheOnly) throws SQLException {
        return getField(symbol, clazz, isCacheOnly, true);
    }

    @SuppressWarnings("unchecked")
    private <T> T getField(Symbol symbol, Class<T> clazz, boolean isReferenceCacheOnly, boolean throwSqlException) throws SQLException {
        refresh();

        Field field = fields.get(symbol);
        if (field == null) {
            return null;
        }

        Object value = field.getValue();

        if (value != null) {
            if (isRecordSubclass(clazz)) {
                // Load foreign key
                if ((field.getReference() == null) && !isReferenceCacheOnly) {
                    try {
                        Record reference = Record.findById((Class<? extends Record>)clazz, value);
                        field.setReference(reference);
                        value = reference;
                    } catch (SQLException e) {
                        if (throwSqlException) {
                            throw e;
                        }
                        throw new RuntimeException("failed to findById(" + clazz + ", " + value + ")", e);
                    }
                } else {
                    value = field.getReference();
                }
            } else if (!clazz.isAssignableFrom(value.getClass())) {
                throw new RuntimeException("column " + symbol.getName() + " is of type " + value.getClass() + ", but " + clazz + " was requested");
            }
        }
        return (T)value;
    }

    /**
     * Provides the value of the field defined by a given column name.
     *
     * @param column
     *            the name of the column defining the field.
     * @throws RuntimeException
     *             if the column does not exist (or has not been set)
     */
    public Object get(String column) {
        return get(Symbol.get(column));
    }

    /**
     * Provides the value of the field defined by a given symbol for a column
     * name.
     *
     * @param symbol
     *            the symbol of the column defining the field.
     * @throws RuntimeException
     *             if the column does not exist (or has not been set)
     */
    public Object get(Symbol symbol) {
        refresh();

        Field field = fields.get(symbol);
        if (field == null) {
            throw new RuntimeException("column '" + symbol.getName() + "' does not exist, or has not yet been set");
        }
        return field.getValue();
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        boolean isFirst = true;

        if (table.getSchema() != null) {
            stringBuilder.append(table.getSchema());
            stringBuilder.append('.');
        }
        if (table.getTable() != null) {
            stringBuilder.append(table.getTable());
            stringBuilder.append(' ');
        }
        if (isStale()) {
            stringBuilder.append("stale ");
        }
        if (isReadOnly()) {
            stringBuilder.append("read-only ");
        }

        stringBuilder.append("{ ");

        for (Entry<Symbol, Field> entry : fields.entrySet()) {
            if (isFirst) {
                isFirst = false;
            } else {
                stringBuilder.append(", ");
            }
            stringBuilder.append(entry.getKey().getName());
            stringBuilder.append(" => ");
            stringBuilder.append(entry.getValue().getValue());
        }
        stringBuilder.append(" }");

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
