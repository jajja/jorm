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
public abstract class Record extends Row {
    private static Map<Class<? extends Record>, Logger> logs = new ConcurrentHashMap<Class<? extends Record>, Logger>(16, 0.75f, 1);

    public static enum ResultMode { // TODO: find a better place for this!
        /** For both INSERTs and UPDATEs, fully repopulate record(s). This is the default. */
        REPOPULATE,
        /** For INSERTs, fetch only generated keys, mark record(s) as stale. For UPDATEs, this is equivalent to NO_RESULT. */
        ID_ONLY,
        /** Fetch nothing, mark record as stale and assume the primary key value is accurate. */
        NO_RESULT;
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

    /**
     * Constructs a record. Uses {@link Jorm} annotation for configuration.
     */
    public Record() {
    }

    /**
     * Constructs a record, using the column values and flags from a Row. Uses {@link Jorm} annotation for configuration.
     *
     * Note #1: this simply copies the column value reference; no deep copying is done. The Row and Record will share column values.
     * Note #2: immutable columns are flagged as immutable.
     */
    public Record(Row row) {
        this.columns = row.columns;
        this.flags = row.flags;
        for (Entry<Symbol, Column> e : this.columns.entrySet()) {
            Symbol symbol = e.getKey();
            Column column = e.getValue();
            column.setImmutable(table().isImmutable(symbol));
        }
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

    @Override
    Column createColumn(Symbol symbol) {
        Column column = new Column();
        columns.put(symbol, column);
        column.setImmutable(table().isImmutable(symbol));
        return column;
    }

    public Value id() {
        return get(primaryKey());
    }

    public Composite primaryKey() {
        return Table.get(getClass()).getPrimaryKey();
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
    public boolean populateByCompositeValue(Value value) throws SQLException {
        return transaction().populateByCompositeValue(this, value);
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
    public static <T extends Record> T find(Class<T> clazz, Value value) throws SQLException {
        return transaction(clazz).find(clazz, value);
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

    public static <T extends Record> List<T> findAll(Class<T> clazz) throws SQLException {
        return transaction(clazz).findAll(clazz);
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
    public static <T extends Record> T find(Class<T> clazz, Object id) throws SQLException {
        return transaction(clazz).find(clazz, id);
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

    public void delete(ResultMode mode) throws SQLException {
        transaction().delete(this, mode);
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
     * Updates the record's changed column values by executing an SQL UPDATE query.
     *
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public int update(ResultMode mode, Composite composite) throws SQLException {
        return transaction().update(this, mode, composite);
    }

    public int update(Composite composite) throws SQLException {
        return transaction().update(this, composite);
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
     * Returns true if specified class is a subclass of Record.class.
     */
    public static boolean isRecordSubclass(Class<?> clazz) {
        return Record.class.isAssignableFrom(clazz) && !clazz.equals(Record.class);
    }

    /**
     * Re-populates a stale record with fresh database values by a select query.
     * A record is considered stale after a call to either
     * {@link Record#insert()} or {@link Record#insert()}, if the SQL dialect of
     * the mapped database does not support RETURNING/OUTPUT. A record mapped to a
     * table in a Postgres database is thus never stale.
     *
     * @throws RuntimeException
     *             whenever a SQLException occurs.
     */
    public Record refresh() throws SQLException {
        if (isStale()) {
            stale(false);
            try {
                assertPrimaryKeyNotNull();
                transaction().populateById(this, primaryKey().valueFrom(this, true));
            } catch (RuntimeException e) {
                stale(true);
                throw e;
            } catch (SQLException e) {
                stale(true);
                throw e;
            }
        }
        return this;
    }

    /**
     * Marks all columns (except immutable, and the primary key) as changed.
     */
    @Override
    public void taint() {
        for (Entry<Symbol, Column> entry : columns.entrySet()) {
            Symbol symbol = entry.getKey();
            Column column = entry.getValue();
            if (!column.isImmutable() && !primaryKey().contains(symbol)) {
                column.setChanged(true);
            }
        }
    }

    private void warnIfStale(Object ref) {
        if (isStale()) {
            log().warn(String.format("Attempt to access %s on stale record %s! refresh() will be called for you, " +
                        "but this functionality will be removed in the future. Update your code to call record.refresh() " +
                        "or record.stale(false) before accessing update()d or insert()d records.", ref, toString()),
                        new IllegalAccessException());
            try {
                refresh();
            } catch (SQLException e) {
                throw new RuntimeException("Failed to refresh stale record", e);
            }
        }
    }

    @Override
    public boolean isCompositeKeyNullOrChanged(Composite key) {
        warnIfStale(key);
        return super.isCompositeKeyNullOrChanged(key);
    }

    @Override
    public boolean isCompositeKeyNull(Composite key) {
        warnIfStale(key);
        return super.isCompositeKeyNull(key);
    }

    @Override
    public boolean isSet(Symbol symbol) {
        warnIfStale(symbol);
        return super.isSet(symbol);
    }

    @Override
    public void unset(Symbol symbol) {
        assertNotReadOnly();
        warnIfStale(symbol);
        super.unset(symbol);
    }

    @Override
    <T> T getColumnValue(Symbol symbol, Class<T> clazz, boolean isReferenceCacheOnly, boolean throwSqlException, Transaction transaction) throws SQLException {
        warnIfStale(symbol);
        return super.getColumnValue(symbol, clazz, isReferenceCacheOnly, throwSqlException, transaction);
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
