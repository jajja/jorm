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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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

    // Helper method used by batch methods to quickly determine a list's generic type
    private static Class<? extends Record> genericType(Iterable<? extends Record> records) {
        Iterator<? extends Record> iter = records.iterator();
        if (iter.hasNext()) {
            return iter.next().getClass();
        }
        throw new IllegalArgumentException("List is empty");
    }

    private static boolean isEmpty(Iterable<?> i) {
        return !i.iterator().hasNext();
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
        return Database.open(table().getDatabase());
    }

    /**
     * Populates the record with the first result for which the given value matches.
     *
     * @param value
     *            the value to match.
     * @return true if the record could be updated with a matching row from the
     *         table.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public boolean populateByCompositeValue(Value value) throws SQLException {
        return transaction().populateByCompositeValue(this, value);
    }

    /**
     * Populates the record with the result for which the id field matches the
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
     * @throws SQLException
     */
    public Query build(String sql) throws SQLException {
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
     * @throws SQLException
     */
    public Query build(String sql, Object... params) throws SQLException {
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
     * @throws SQLException
     */
    public static Query build(Class<? extends Record> clazz, String sql) throws SQLException {
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
     * @throws SQLException
     */
    public static Query build(Class<? extends Record> clazz, String sql, Object... params) throws SQLException {
        return transaction(clazz).build(sql, params);
    }

    /**
     * Provides a selected record from the mapped database table, populated with
     * the first result for which the primary key value, or composite value, matches.
     *
     * @param clazz
     *            the class defining the table mapping.
     * @param value
     *            the primary key value, or a composite value
     * @return the matched record or null for no match.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static <T extends Record> T find(Class<T> clazz, Object value) throws SQLException {
        return transaction(clazz).find(clazz, value);
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
    public static <T extends Record> List<T> findAll(Class<T> clazz, Value value) throws SQLException {
        return transaction(clazz).findAll(clazz, value);
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

    public static RecordIterator iterate(Class<? extends Record> clazz, Value value) throws SQLException {
        return transaction(clazz).iterate(clazz, value);
    }

    public static RecordIterator iterate(Class<? extends Record> clazz) throws SQLException {
        return transaction(clazz).iterate(clazz);
    }

    /**
     * Provides a selected record, populated with the result for which the primary key
     * field matches the given id value.
     *
     * @param clazz
     *            the class defining the table mapping.
     * @param id
     *            the primary key value (can be either a {@link Composite.Value} or a simple value).
     * @return the matched record or null for no match.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static <T extends Record> T findById(Class<T> clazz, Object id) throws SQLException {
        return transaction(clazz).findById(clazz, id);
    }

    public static int deleteById(Class<? extends Record> clazz, Object id) throws SQLException {
        return transaction(clazz).deleteById(clazz, id);
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
    public static <T extends Record> Map<Composite.Value, T> selectAsMap(Class<T> clazz, Object key, boolean allowDuplicates, Query query) throws SQLException {
        return transaction(clazz).selectAsMap(clazz, key, allowDuplicates, query);
    }

    public static <T extends Record> Map<Composite.Value, T> selectAsMap(Class<T> clazz, Object key, boolean allowDuplicates, String sql, Object... params) throws SQLException {
        return transaction(clazz).selectAsMap(clazz, key, allowDuplicates, sql, params);
    }

    @Deprecated
    public static <T extends Record> Map<Composite.Value, T> selectAsMap(Class<T> clazz, Object key, boolean allowDuplicates) throws SQLException {
        return transaction(clazz).selectAsMap(clazz, key, allowDuplicates);
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
    public static <T, C extends Record> Map<T, C> selectAsTypedMap(Class<C> clazz, Object key, Class<T> keyType, boolean allowDuplicates, Query query) throws SQLException {
        return transaction(clazz).selectAsTypedMap(clazz, key, keyType, allowDuplicates, query);
    }

    public static <T, C extends Record> Map<T, C> selectAsTypedMap(Class<C> clazz, Object key, Class<T> keyType, boolean allowDuplicates, String sql, Object... params) throws SQLException {
        return transaction(clazz).selectAsTypedMap(clazz, key, keyType, allowDuplicates, sql, params);
    }

    @Deprecated
    public static <T, C extends Record> Map<T, C> selectAsTypedMap(Class<C> clazz, Object key, Class<T> keyType, boolean allowDuplicates) throws SQLException {
        return transaction(clazz).selectAsTypedMap(clazz, key, keyType, allowDuplicates);
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
    public static <T extends Record> Map<Composite.Value, List<T>> selectAllAsMap(Class<T> clazz, Object key, Query query) throws SQLException {
        return transaction(clazz).selectAllAsMap(clazz, key, query);
    }

    public static <T extends Record> Map<Composite.Value, List<T>> selectAllAsMap(Class<T> clazz, Object key, String sql, Object... params) throws SQLException {
        return transaction(clazz).selectAllAsMap(clazz, key, sql, params);
    }

    @Deprecated
    public static <T extends Record> Map<Composite.Value, List<T>> selectAllAsMap(Class<T> clazz, Object key) throws SQLException {
        return transaction(clazz).selectAllAsMap(clazz, key);
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
    public static <T, C extends Record> Map<T, List<C>> selectAllAsTypedMap(Class<C> clazz, Object key, Class<T> keyType, Query query) throws SQLException {
        return transaction(clazz).selectAllAsTypedMap(clazz, key, keyType, query);
    }

    public static <T, C extends Record> Map<T, List<C>> selectAllAsTypedMap(Class<C> clazz, Object key, Class<T> keyType, String sql, Object... params) throws SQLException {
        return transaction(clazz).selectAllAsTypedMap(clazz, key, keyType, sql, params);
    }

    @Deprecated
    public static <T, C extends Record> Map<T, List<C>> selectAllAsTypedMap(Class<C> clazz, Object key, Class<T> keyType) throws SQLException {
        return transaction(clazz).selectAllAsTypedMap(clazz, key, keyType);
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
     * Populates all records in the given iterable of records with a single
     * prefetched reference of the given record class. Existing cached
     * references are not overwritten.
     *
     * @param records
     *            the records to populate with prefetched references.
     * @param foreignKeyColumn
     *            the column name defining the foreign key to the referenced records.
     * @param clazz
     *            the class of the referenced records.
     * @param referredColumn
     *            the column name defining the referred column of the referenced
     *            records.
     * @return the prefetched records.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static <T extends Record> Map<Composite.Value, T> prefetch(Iterable<? extends Record> records, String foreignKeyColumn, Class<T> clazz, String referredColumn) throws SQLException {
        if (!isEmpty(records)) {
            return transaction(clazz).prefetch(records, foreignKeyColumn, clazz, referredColumn);
        }
        return new HashMap<Composite.Value, T>();
    }

    public static <T extends Record> Map<Composite.Value, T> prefetch(Iterable<? extends Record> records, String foreignKeyColumn, Class<T> clazz, String referredColumn, boolean ignoreInvalidReferences) throws SQLException {
        if (!isEmpty(records)) {
            return transaction(clazz).prefetch(records, foreignKeyColumn, clazz, referredColumn, ignoreInvalidReferences);
        }
        return new HashMap<Composite.Value, T>();
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
     * Save the record. This is done by a call to {@link #insert()} if the id
     * column is null, unset or changed, otherwise by a call to {@link #update()}.
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
     * column is null, unset or changed, otherwise by a call to {@link #update()}.
     *
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static void save(Iterable<? extends Record> records, int batchSize, ResultMode mode) throws SQLException {
        if (!isEmpty(records)) {
            transaction(genericType(records)).save(records, batchSize, mode);
        }
    }

    /**
     * Batch saves the records. This is done by a call to {@link #insert()} if the id
     * column is null, unset or changed, otherwise by a call to {@link #update()}.
     *
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static void save(Iterable<? extends Record> records) throws SQLException {
        if (!isEmpty(records)) {
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
    public static void delete(Iterable<? extends Record> records) throws SQLException {
        if (!isEmpty(records)) {
            transaction(genericType(records)).delete(records);
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
    public static void insert(Iterable<? extends Record> records, ResultMode mode) throws SQLException {
        if (!isEmpty(records)) {
            transaction(genericType(records)).insert(records, mode);
        }
    }

    public static void insert(Iterable<? extends Record> records) throws SQLException {
        if (!isEmpty(records)) {
            transaction(genericType(records)).insert(records);
        }
    }

    /**
     * Executes a batch INSERT (INSERT INTO ... (columns...) VALUES (row1), (row2), (row3), ...).
     *
     * For large sets of records, the use of chunkSize is recommended to avoid out-of-memory errors and too long SQL queries.
     *
     * Setting isFullRepopulate to true will re-populate the record columns with fresh values. This will generate
     * an additional SELECT query for every chunk of records for databases that do not support RETURNING.
     *
     * @param records List of records to insert (must be of the same class, and bound to the same Database)
     * @param chunkSize Splits the records into chunks, <= 0 disables
     * @param isFullRepopulate Whether or not to fully re-populate the record columns, or just update their primary key value
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static void insert(Iterable<? extends Record> records, int chunkSize, ResultMode mode) throws SQLException {
        if (!isEmpty(records)) {
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
    public int update(ResultMode mode, Composite primaryKey) throws SQLException {
        return transaction().update(this, mode, primaryKey);
    }

    /**
     * Updates the record's changed column values by executing an SQL UPDATE query.
     *
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public int update(ResultMode mode) throws SQLException {
        return transaction().update(this, mode);
    }

    public int update() throws SQLException {
        return transaction().update(this);
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
    public static void update(Iterable<? extends Record> records) throws SQLException {
        if (!isEmpty(records)) {
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
    public static void update(Iterable<? extends Record> records, int chunkSize, ResultMode mode) throws SQLException {
        if (!isEmpty(records)) {
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
    public static void update(Iterable<? extends Record> records, int chunkSize, ResultMode mode, Composite primaryKey) throws SQLException {
        if (!isEmpty(records)) {
            transaction(genericType(records)).update(records, chunkSize, mode, primaryKey);
        }
    }

    /**
     * Returns true if specified class is a subclass of Record.class.
     */
    public static boolean isRecordSubclass(Class<?> clazz) {
        return Record.class.isAssignableFrom(clazz) && !clazz.equals(Record.class);
    }

    /**
     * Re-populates a record with fresh database values by a select query.
     *
     * @throws RuntimeException
     *             whenever a SQLException occurs.
     */
    public Record refresh() throws SQLException {
        assertPrimaryKeyNotNull();
        populateById(primaryKey().valueFrom(this, true));
        return this;
    }

    public boolean isImmutable(String column) {
        return table().isImmutable(column);
    }

    public boolean isDirty(String column) {
        return isChanged(column) && !isImmutable(column);
    }

    /**
     * Marks all fields as changed, excluding any immutable and primary key columns.
     */
    public void taint() {
        for (Entry<String, Field> e : fields.entrySet()) {
            String column = e.getKey();
            Field field = e.getValue();
            if (!isImmutable(column) && !primaryKey().contains(column)) {
                field.setChanged(true);
            }
        }
    }

    /**
     * Checks whether any mutable fields have changed since the last call to populate().
     *
     * @return true if at least one mutable field has changed, false otherwise
     */
    public boolean isDirty() {
        for (Entry<String, Field> e : fields.entrySet()) {
            String column = e.getKey();
            Field field = e.getValue();
            if (field.isChanged() && !isImmutable(column)) {
                return true;
            }
        }
        return false;
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
