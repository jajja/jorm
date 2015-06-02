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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jajja.jorm.Composite.Value;
import com.jajja.jorm.Record.ResultMode;
import com.jajja.jorm.Row.Column;
import com.jajja.jorm.sql.Language;
import com.jajja.jorm.sql.Language.Batch;

/**
 * The transaction implementation executing all queries in for {@link Jorm}
 * mapped and anonymous records alike.
 *
 * <h3>Lifecycle</h3>
 * <ul>
 * <li><tt>Begin&nbsp;&nbsp;:&nbsp;Dormant&nbsp;->&nbsp;Active</tt></li>
 * <li><tt>Commit&nbsp;:&nbsp;Active&nbsp;&nbsp;->&nbsp;Active</tt></li>
 * <li><tt>Close&nbsp;&nbsp;:&nbsp;Active&nbsp;&nbsp;->&nbsp;Dormant</tt></li>
 * </ul>
 * The lifecycle begins with a dormant phase. The Begin transition is implicitly
 * defined by the first query executed, either through a record or anonymously
 * through a transaction, and the active phase is entered. Setting a savepoint
 * on a dormant transaction has the same effect. The Commit transition
 * propagates any changes to the database, which automatically resets the
 * transaction to a new active phase. The Commit transition is explicitly
 * defined by a call to {@link #commit()} The Close transition rolls back the
 * unpropagated changes to the database from the current active phase and enters
 * a dormant phase. The Close transition is explicitly defined by a call to
 * {@link #close()}, which is automatically propagated by
 * {@link Database#close()}.
 *
 * @see Query
 * @see Database
 * @see Record
 * @author Martin Korinth <martin.korinth@jajja.com>
 * @author Andreas Allerdahl <andreas.allerdahl@jajja.com>
 * @author Daniel Adolfsson <daniel.adolfsson@jajja.com>
 * @since 1.0.0
 */
public class Transaction {
    private static Logger log = LoggerFactory.getLogger(Transaction.class);
    private final String database;
    private final DataSource dataSource;
    private Language language;
    private Connection connection;
    private boolean isDestroyed = false;
    private boolean isLoggingEnabled = false;
    private Calendar calendar;

    private void tracelog(String message) {
        if (isLoggingEnabled) {
            StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
            for (int i = 1; i < stackTrace.length; i++) {
                StackTraceElement stackTraceElement = stackTrace[i];
                if (!stackTraceElement.getClassName().startsWith("com.jajja.jorm.")) {
                    System.out.println(stackTraceElement + ": " + message);
                    log.info(stackTraceElement + ": " + message);
                    break;
                }
            }
        }
    }

    public void setCalendar(Calendar calendar) {
        this.calendar = calendar;
    }

    public Calendar getCalendar() {
        return calendar;
    }

    public void setTimeZone(TimeZone timeZone) {
        this.calendar = Calendar.getInstance(timeZone);
    }

    Transaction(DataSource dataSource, String database, Calendar calendar) {
        this.database = database;
        this.dataSource = dataSource;
        this.calendar = calendar;
    }

    /**
     * Provides the name of the database for the transaction.
     *
     * @return the name of the database
     */
    public String getDatabase() {
        return database;
    }

    /**
     * Provides the data source fro the transaction.
     *
     * @return the data source.
     */
    public DataSource getDataSource() {
        return dataSource;
    }

    /**
     * Provides the SQL dialect of the connection for the transaction.
     */
    public Language getLanguage() {
        if (language == null) {
            try {
                language = Language.get(connection);
            } catch (SQLException sqlException) {
                throw new RuntimeException("Failed to get database connection", sqlException);
            }
        }
        return language;
    }

    /**
     * Enables or disables SQL query logging on this transaction.
     */
    public void setLoggingEnabled(boolean loggingEnabled) {
        this.isLoggingEnabled = loggingEnabled;
    }

    /**
     * Provides the current connection, opening a new if needed. Disables auto
     * commit.
     *
     * @return the connection of the transaction.
     * @throws SQLException
     *             if a database access error occurs.
     */
    public Connection getConnection() throws SQLException {
        if (isDestroyed) {
            throw new IllegalStateException("Attempted to use destroyed transaction!");
        }
        if (connection == null) {
            connection = dataSource.getConnection();
            tracelog("BEGIN");
            connection.setAutoCommit(false);
        }
        return connection;
    }

    /**
     * Rolls back the current transaction and closes the database connection.
     * This is the equivalent of calling {@link #rollback()}
     */
    public void close() {
        rollback();
    }

    /**
     * Destroys the transaction.
     */
    public void destroy() {
        close();
        isDestroyed = true;
    }

    /**
     * Commits the current transaction and closes the database connection.
     * This is the equivalent of calling {@link #commit(true)}.
     *
     * @throws SQLException
     *             if a database access error occurs.
     */
    public void commit() throws SQLException {
        commit(true);
    }

    /**
     * Commits the current transaction and optionally closes the database connection.
     *
     * @param close
     *            whether or not to close the database connection.
     * @throws SQLException
     *             if a database access error occurs.
     */
    public void commit(boolean close) throws SQLException {
        if (connection != null) {
            tracelog("COMMIT");
            getConnection().commit();
            if (close) {
                close();
            }
        }
    }

    /**
     * Rolls back the current transaction and closes the database connection.
     *
     * @param close
     *            whether or not to close the database connection.
     */
    public void rollback() {
        rollback(true);
    }

    /**
     * Rolls back the current transaction and optionally closes the database connection.
     *
     * @param close
     *            whether or not to close the database connection.
     */
    public void rollback(boolean close) {
        if (connection != null) {
            try {
                tracelog("ROLLBACK");
                connection.rollback();
            } catch (SQLException e) {
                log.error("Failed to rollback transaction", e);
            }
            try {
                connection.close();
            } catch (SQLException e) {
                log.error("Failed to close connection", e);
            }
            language = null;
            connection = null;
        }
    }

    /**
     * Provides a prepared statement for the given query.
     *
     * @param query
     *            the query.
     * @throws SQLException
     *             if a database access error occurs.
     */
    public PreparedStatement prepare(Query query) throws SQLException {
        return prepare(query, false);
    }

    /**
     * Provides a prepared statement for the given query.
     *
     * @param query
     *            the query.
     * @param returnGeneratedKeys sets Statement.RETURN_GENERATED_KEYS if true
     * @throws SQLException
     *             if a database access error occurs.
     */
    public PreparedStatement prepare(Query query, boolean returnGeneratedKeys) throws SQLException {
        return prepare(query.getSql(), query.getParams(), returnGeneratedKeys);
    }

    /**
     * Provides a prepared statement for the query given by a JDBC SQL statement and applicable parameters.
     *
     * @param sql
     *            the JDBC SQL statement.
     * @param params
     *            the applicable parameters.
     * @throws SQLException
     *             if a database access error occurs.
     */
    public PreparedStatement prepare(String sql, Collection<Object> params) throws SQLException {
        return prepare(sql, params, false);
    }

    /**
     * Provides a prepared statement for the query given by a JDBC SQL statement and applicable parameters.
     *
     * @param sql
     *            the JDBC SQL statement.
     * @param params
     *            the applicable parameters.
     * @param returnGeneratedKeys sets Statement.RETURN_GENERATED_KEYS if true
     * @throws SQLException
     *             if a database access error occurs.
     */
    public PreparedStatement prepare(String sql, Collection<Object> params, boolean returnGeneratedKeys) throws SQLException {
        tracelog(sql);
        try {
            PreparedStatement preparedStatement = getConnection().prepareStatement(sql, returnGeneratedKeys ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS);
            if (params != null) {
                int p = 1;
                for (Object param : params) {
                    if (calendar != null) {
                        if (param instanceof Date) {
                            preparedStatement.setDate(p++, (Date)param, calendar);
                        } else if (param instanceof Time) {
                            preparedStatement.setTime(p++, (Time)param, calendar);
                        } else if (param instanceof Timestamp) {
                            preparedStatement.setTimestamp(p++, (Timestamp)param, calendar);
                        } else {
                            preparedStatement.setObject(p++, param);
                        }
                    } else {
                        preparedStatement.setObject(p++, param);
                    }
                }
            }
            return preparedStatement;
        } catch (SQLException e) {
            throw getLanguage().rethrow(e, sql);
        }
    }

    /**
     * Executes the update query given by a Jorm SQL statement and applicable parameters.
     *
     * @param sql
     *            the Jorm SQL statement.
     * @param params
     *            the applicable parameters.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public void execute(String sql, Object... params) throws SQLException {
        execute(build(sql, params));
    }

    /**
     * Executes the given query.
     *
     * @param query
     *            the query.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public void execute(Query query) throws SQLException {
        PreparedStatement preparedStatement = prepare(query.getSql(), query.getParams());
        try {
            preparedStatement.execute();
        } catch (SQLException sqlException) {
            throw getLanguage().rethrow(sqlException, query.getSql());
        } finally {
            preparedStatement.close();
        }
    }

    /**
     * Executes the update query given by a Jorm SQL statement and applicable parameters.
     *
     * @param sql
     *            the Jorm SQL statement.
     * @param params
     *            the applicable parameters.
     * @return the number of updated rows in the database.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public int executeUpdate(String sql, Object ... params) throws SQLException {
        return executeUpdate(build(sql, params));
    }

    /**
     * Executes the given update query.
     *
     * @param query
     *            the update query.
     * @return the number of updated rows in the database.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public int executeUpdate(Query query) throws SQLException {
        PreparedStatement preparedStatement = prepare(query.getSql(), query.getParams());
        try {
            return preparedStatement.executeUpdate();
        } catch (SQLException sqlException) {
            throw getLanguage().rethrow(sqlException, query.getSql());
        } finally {
            preparedStatement.close();
        }
    }

    /**
     * Provides a list of selected anonymous read-only records, populated with
     * the results from the given query.
     *
     * @param query
     *            the query.
     * @return the matched records.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public Map<Composite.Value, Row> selectAsMap(Composite compositeKey, boolean allowDuplicates, Query query) throws SQLException {
        return selectAsMap(Row.class, compositeKey, allowDuplicates, query);
    }

    public Map<Composite.Value, Row> selectAsMap(Composite compositeKey, boolean allowDuplicates, String sql, Object ... params) throws SQLException {
        return selectAsMap(compositeKey, allowDuplicates, build(sql, params));
    }

    /**
     * Provides a list of selected anonymous read-only records, populated with
     * the results from the given query.
     *
     * @param query
     *            the query.
     * @return the matched records.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public Map<Composite.Value, List<Row>> selectAllAsMap(Composite composite, Query query) throws SQLException {
        return selectAllAsMap(Row.class, composite, query);
    }

    public Map<Composite.Value, List<Row>> selectAllAsMap(Composite compositeKey, String sql, Object ... params) throws SQLException {
        return selectAllAsMap(compositeKey, build(sql, params));
    }

    /**
     * Provides an anonymous read-only record, populated with the first result
     * from the query given by a Jorm SQL statement and applicable parameters.
     *
     * @param sql
     *            the Jorm SQL statement.
     * @param params
     *            the applicable parameters.
     * @return the matched record or null for no match.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public Row select(String sql, Object ... params) throws SQLException {
        return select(build(sql, params));
    }

    /**
     * Provides an anonymous read-only record, populated with the first result from the
     * given query.
     *
     * @param query
     *            the query.
     * @return the matched record or null for no match.
     * @throws SQLException
     *             if a database access error occurs
     */
    public Row select(Query query) throws SQLException {
        Row row = new Row();
        if (!selectInto(row, query)) {
            row = null;
        }
        return row;
    }

    /**
     * Provides a list of selected anonymous read-only records, populated with
     * the results from the query given by a Jorm SQL statement and applicable
     * parameters.
     *
     * @param sql
     *            the Jorm SQL statement.
     * @param params
     *            the applicable parameters.
     * @return the matched records
     * @throws SQLException
     *             if a database access error occurs
     */
    public List<Row> selectAll(String sql, Object ... params) throws SQLException {
        return selectAll(build(sql, params));
    }

    /**
     * Provides a list of {@link Row}s populated with the results from the given query.
     *
     * @param query
     *            the query.
     * @return the matched records.
     * @throws SQLException
     *             if a database access error occurs
     */
    public List<Row> selectAll(Query query) throws SQLException {
        List<Row> rows = new LinkedList<Row>();
        try {
            RecordIterator iter = null;
            try {
                iter = new RecordIterator(this, prepare(query.getSql(), query.getParams()));
                while (iter.next()) {
                    rows.add(iter.row());
                }
            } finally {
                if (iter != null) iter.close();
            }
        } catch (SQLException e) {
            throw getLanguage().rethrow(e, query.getSql());
        }
        return rows;
    }

    /**
     * Provides an iterator for the results from the given query.
     *
     * @param query
     *            the query.
     * @return the matched records.
     * @throws SQLException
     *             if a database access error occurs
     */
    public RecordIterator iterate(Query query) throws SQLException {
        return new RecordIterator(this, prepare(query.getSql(), query.getParams()));
    }

    /**
     * Provides an iterator for the results from the given query.
     *
     * @param query
     *            the query.
     * @return the matched records.
     * @throws SQLException
     *             if a database access error occurs
     */
    public RecordIterator iterate(String sql, Object ... params) throws SQLException {
        return iterate(build(sql, params));
    }

    /**
     * Sets an unnamed savepoint on the active transaction. If the transaction
     * is dormant it begins and enters the active state.
     *
     * @return the savepoint.
     * @throws SQLException
     * @throws SQLException
     *             if a database access error occurs.
     */
    public Savepoint savepoint() throws SQLException {
        return getConnection().setSavepoint();
    }

    /**
     * Sets an named savepoint on the active transaction. If the transaction is
     * dormant it begins and enters the active state.
     *
     * @param name
     *            the name of the savepoint.
     * @return the savepoint.
     * @throws SQLException
     * @throws SQLException
     *             if a database access error occurs.
     */
    public Savepoint savepoint(String name) throws SQLException {
        return getConnection().setSavepoint(name);
    }

    /**
     * Removes the savepoint and any subsequent savepoints from the transaction.
     * Any reference to a removed savepoint will cause a SQL exception to be
     * thrown. If the transaction is dormant releasing a savepoint has no effect
     * and the state of the transaction is unchanged.
     *
     * @param savepoint
     *            the savepoint to release.
     * @throws SQLException
     *             if a database access error occurs or a removed savepoint was
     *             referenced.
     */
    public void release(Savepoint savepoint) throws SQLException {
        if (connection != null) {
            connection.releaseSavepoint(savepoint);
        }
    }

    /**
     * Resets all changes made after the given savepoint. Removes any subsequent
     * savepoints from the transacion. Any reference to a removed savepoint will
     * cause a SQL exception to be thrown. If the transaction is dormant,
     * rollback to a savepoint has no effect and the state of the transaction is
     * unchanged.
     *
     * @param savepoint
     *            the savepoint to rollback the transaction to.
     * @throws SQLException
     *             if a database access error occurs or a removed savepoint was
     *             referenced.
     */
    public void rollback(Savepoint savepoint) throws SQLException {
        if (connection != null) {
            connection.rollback(savepoint);
        }
    }

    public void load(InputStream inputStream) throws IOException, SQLException {
        load(inputStream, "UTF-8");
    }

    public void load(InputStream inputStream, String charset) throws IOException, SQLException {
        BufferedReader bufferedReader = null;
        Statement statement = null;
        try {
            statement = getConnection().createStatement();
            StringBuilder sql = new StringBuilder();
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream, charset));
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                sql.append(line);
            }
            statement.execute(sql.toString());
        } catch (SQLException e) {
            throw getLanguage().rethrow(e);
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
            } finally {
                if (bufferedReader != null) {
                    bufferedReader.close();
                }
            }
        }
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
    public boolean populateByCompositeValue(Record record, Value value) throws SQLException {
        return selectInto(record, getLanguage().buildSelectQuery(record.table(), value));
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
    public boolean populateById(Record record, Object id) throws SQLException {
        return populateByCompositeValue(record, Record.primaryKey(record.getClass()).value(id));
    }

    /**
     * Builds a generic SQL query for the record. XXX redoc
     *
     * @param sql
     *            the SQL statement to represent the query.
     * @return the built query.
     */
    public Query build() {
        return new Query(getLanguage());
    }

    /**
     * Builds a generic SQL query for the record.
     *
     * @param sql
     *            the SQL statement to represent the query.
     * @return the built query.
     */
    public Query build(String sql) {
        return new Query(getLanguage(), sql);
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
        return new Query(getLanguage(), sql, params);
    }

    /**
     * Provides a selected record from the mapped database table, populated with
     * the first result for which the primary key value, or composite value, matches.
     *
     * @param clazz
     *            the class defining the table mapping.
     * @param value
     *            the primary key value, or composite value
     * @return the matched record or null for no match.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public <T extends Record> T find(Class<T> clazz, Value value) throws SQLException {
        return select(clazz, getLanguage().buildSelectQuery(Table.get(clazz), value));
    }

    /**
     * Provides a complete list of selected records from the mapped database
     * table, populated with the results for which the composite value matches.
     *
     * @param clazz
     *            the class defining the table mapping.
     * @param composite
     *            the composite key
     * @param value
     *            the composite value
     * @return the matched records.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public <T extends Record> List<T> findAll(Class<T> clazz, Value value) throws SQLException {
        return selectAll(clazz, getLanguage().buildSelectQuery(Table.get(clazz), value));
    }

    public <T extends Record> List<T> findAll(Class<T> clazz) throws SQLException {
        return selectAll(clazz, getLanguage().buildSelectQuery(Table.get(clazz), null));
    }

    public RecordIterator iterate(Class<? extends Record> clazz, Value value) throws SQLException {
        return selectIterator(clazz, getLanguage().buildSelectQuery(Table.get(clazz), value));
    }

    public RecordIterator iterate(Class<? extends Record> clazz) throws SQLException {
        return selectIterator(clazz, getLanguage().buildSelectQuery(Table.get(clazz), null));
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
    public <T extends Record> T find(Class<T> clazz, Object id) throws SQLException {
        if (id instanceof Value) {
            return find(clazz, (Value)id);
        } else {
            return find(clazz, Record.primaryKey(clazz).value(id));
        }
    }

    public int delete(Class<? extends Record> clazz, Object id) throws SQLException {
        if (id instanceof Value) {
            return delete(clazz, (Value)id);
        } else {
            return delete(clazz, Record.primaryKey(clazz).value(id));
        }
    }

    public int delete(Class<? extends Record> clazz, Value value) throws SQLException {
        return executeUpdate(getLanguage().buildDeleteQuery(Table.get(clazz), value));
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
    public <T extends Record> T select(Class<T> clazz, String sql) throws SQLException {
        return select(clazz, build(sql));
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
    public <T extends Record> T select(Class<T> clazz, String sql, Object... params) throws SQLException {
        return select(clazz, build(sql, params));
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
    public <T extends Record> T select(Class<T> clazz, Query query) throws SQLException {
        T record = Record.construct(clazz);
        if (selectInto(record, query)) {
            return record;
        }
        return null;
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
    public <T extends Record> List<T> selectAll(Class<T> clazz, String sql) throws SQLException {
        return selectAll(clazz, build(sql));
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
    public <T extends Record> List<T> selectAll(Class<T> clazz, String sql, Object... params) throws SQLException {
        return selectAll(clazz, build(sql, params));
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
    public <T extends Record> List<T> selectAll(Class<T> clazz, Query query) throws SQLException {
        List<T> records = new LinkedList<T>();
        try {
            RecordIterator iter = null;
            try {
                iter = new RecordIterator(this, prepare(query));
                while (iter.next()) {
                    records.add(iter.record(clazz));
                }
            } finally {
                if (iter != null) iter.close();
            }
        } catch (SQLException e) {
            throw getLanguage().rethrow(e, query.getSql());
        }
        return records;
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
    public RecordIterator selectIterator(Class<? extends Record> clazz, String sql) throws SQLException {
        return selectIterator(clazz, build(sql));
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
    public RecordIterator selectIterator(Class<? extends Record> clazz, String sql, Object... params) throws SQLException {
        return selectIterator(clazz, build(sql, params));
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
    public RecordIterator selectIterator(Class<? extends Record> clazz, Query query) throws SQLException {
        try {
            return new RecordIterator(this, prepare(query));
        } catch (SQLException e) {
            throw getLanguage().rethrow(e, query.getSql());
        }
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
    public <T extends Row> Map<Composite.Value, T> selectAsMap(Class<T> clazz, Composite compositeKey, boolean allowDuplicates, Query query) throws SQLException {
        HashMap<Composite.Value, T> map = new HashMap<Composite.Value, T>();
        try {
            RecordIterator iter = null;
            try {
                boolean wantRecords = Record.isRecordSubclass(clazz);
                iter = new RecordIterator(this, prepare(query));
                while (iter.next()) {
                    @SuppressWarnings("unchecked")
                    T row = (T)(wantRecords ? iter.record((Class<? extends Record>)clazz) : iter.row());
                    Composite.Value value = compositeKey.valueFrom(row);
                    if (map.put(value, row) != null && !allowDuplicates) {
                        throw new IllegalStateException("Duplicate key " + value);
                    }
                }
            } finally {
                if (iter != null) iter.close();
            }
        } catch (SQLException e) {
            throw getLanguage().rethrow(e, query.getSql());
        }
        return map;
    }

    public <T extends Record> Map<Composite.Value, T> selectAsMap(Class<T> clazz, Composite compositeKey, boolean allowDuplicates, String sql, Object... params) throws SQLException {
        return selectAsMap(clazz, compositeKey, allowDuplicates, build(sql, params));
    }

    public <T extends Record> Map<Composite.Value, T> selectAsMap(Class<T> clazz, Composite compositeKey, boolean allowDuplicates) throws SQLException {
        return selectAsMap(clazz, compositeKey, allowDuplicates, getLanguage().buildSelectQuery(Table.get(clazz), null));
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
    public <T extends Row> Map<Composite.Value, List<T>> selectAllAsMap(Class<T> clazz, Composite compositeKey, Query query) throws SQLException {
        HashMap<Composite.Value, List<T>> map = new HashMap<Composite.Value, List<T>>();
        try {
            RecordIterator iter = null;
            try {
                boolean wantRecords = Record.isRecordSubclass(clazz);
                iter = new RecordIterator(this, prepare(query));
                while (iter.next()) {
                    @SuppressWarnings("unchecked")
                    T row = (T)(wantRecords ? iter.record((Class<? extends Record>)clazz) : iter.row());
                    Composite.Value value = compositeKey.valueFrom(row);
                    List<T> list = map.get(value);
                    if (list == null) {
                        list = new LinkedList<T>();
                        map.put(value, list);
                    }
                    list.add(row);
                }
            } finally {
                if (iter != null) iter.close();
            }
        } catch (SQLException e) {
            throw getLanguage().rethrow(e, query.getSql());
        }
        return map;
    }

    public <T extends Record> Map<Composite.Value, List<T>> selectAllAsMap(Class<T> clazz, Composite compositeKey, String sql, Object... params) throws SQLException {
        return selectAllAsMap(clazz, compositeKey, build(sql, params));
    }

    public <T extends Record> Map<Composite.Value, List<T>> selectAllAsMap(Class<T> clazz, Composite compositeKey) throws SQLException {
        return selectAllAsMap(clazz, compositeKey, getLanguage().buildSelectQuery(Table.get(clazz), null));
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
    public boolean selectInto(Row row, String sql) throws SQLException {
        return selectInto(row, build(sql));
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
    public boolean selectInto(Row row, String sql, Object... params) throws SQLException {
        return selectInto(row, build(sql, params));
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
    public boolean selectInto(Row row, Query query) throws SQLException {
        try {
            RecordIterator iter = null;
            try {
                iter = new RecordIterator(this, prepare(query));
                if (iter.next()) {
                    iter.populate(row);
                    return true;
                }
            } finally {
                 if (iter != null) iter.close();
            }
        } catch (SQLException e) {
            throw getLanguage().rethrow(e, query.getSql());
        }
        return false;
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
    public <T extends Record> Map<Composite.Value, T> prefetch(Collection<? extends Row> rows, Symbol foreignKeySymbol, Class<T> clazz, Symbol referredSymbol) throws SQLException {
        return prefetch(rows, foreignKeySymbol, clazz, referredSymbol, false);
    }

    public <T extends Record> Map<Composite.Value, T> prefetch(Collection<? extends Row> rows, Symbol foreignKeySymbol, Class<T> clazz, Symbol referredSymbol, boolean ignoreInvalidReferences) throws SQLException {
        Set<Object> values = new HashSet<Object>();

        for (Row row : rows) {
            Column column = row.columns.get(foreignKeySymbol);
            if (column != null && column.getValue() != null && column.getReference() == null) {
                values.add(column.getValue());
            }
        }

        if (values.isEmpty()) {
            return new HashMap<Composite.Value, T>();
        }

        Composite key = new Composite(referredSymbol);
        Map<Composite.Value, T> map = selectAsMap(clazz, key, ignoreInvalidReferences, getLanguage().buildSelectQuery(Table.get(clazz), key, values));

        for (Row row : rows) {
            Column column = row.columns.get(foreignKeySymbol);
            if (column != null && column.getValue() != null && column.getReference() == null) {
                Record referenceRecord = map.get(key.value(column.getValue()));
                if (referenceRecord == null && !ignoreInvalidReferences) {
                    throw new IllegalStateException(column.getValue() + " not present in " + Table.get(clazz).getTable() + "." + referredSymbol.getName());
                }
                row.set(foreignKeySymbol, referenceRecord);
            }
        }

        return map;
    }

    /**
     * Populates all records in the given collection of records with a single
     * prefetched reference of the given record class. Existing cached
     * references are not overwritten.
     *
     * @param rows
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
    // XXX context
    public <T extends Record> Map<Composite.Value, T> prefetch(Collection<? extends Row> rows, String foreignKeySymbol, Class<T> clazz, String referredSymbol) throws SQLException {
        return prefetch(rows, Symbol.get(foreignKeySymbol), clazz, Symbol.get(referredSymbol));
    }

    // XXX context
    public <T extends Record> Map<Composite.Value, T> prefetch(Collection<? extends Row> rows, String foreignKeySymbol, Class<T> clazz, String referredSymbol, boolean ignoreInvalidReferences) throws SQLException {
        return prefetch(rows, Symbol.get(foreignKeySymbol), clazz, Symbol.get(referredSymbol), ignoreInvalidReferences);
    }

    /**
     * Save the record. This is done by a call to {@link #insert()} if the id
     * column is null, unset or changed, otherwise by a call to {@link #update()}.
     *
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public void save(Record record, ResultMode mode) throws SQLException {
        record.assertNotReadOnly();
        if (record.isPrimaryKeyNullOrChanged()) {
            insert(record, mode);
        } else {
            update(record, mode);
        }
    }

    public void save(Record record) throws SQLException {
        save(record, ResultMode.REPOPULATE);
    }

    /**
     * Batch saves the records. This is done by a call to {@link #insert()} if the id
     * column is null, unset or changed, otherwise by a call to {@link #update()}.
     *
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public void save(Collection<? extends Record> records, ResultMode mode) throws SQLException {
        List<Record> insertRecords = new LinkedList<Record>();
        List<Record> updateRecords = new LinkedList<Record>();

        for (Record record : records) {
            if (record.isPrimaryKeyNullOrChanged()) {
                insertRecords.add(record);
            } else {
                updateRecords.add(record);
            }
        }

        insert(insertRecords, mode);
        update(updateRecords, mode);
    }

    /**
     * Batch saves the records. This is done by a call to {@link #insert()} if the id
     * column is null, unset or changed, otherwise by a call to {@link #update()}.
     *
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public void save(Collection<? extends Record> records) throws SQLException {
        save(records, ResultMode.REPOPULATE);
    }

    /**
     * Deletes the record row from the database by executing the SQL query "DELETE FROM [tableName] WHERE [primaryKey] = [primaryKeyColumnValue]".
     * The primary key column value is also set to null.
     *
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public void delete(Record record) throws SQLException {
        delete(record, ResultMode.NO_RESULT);
    }

    public void delete(Record record, ResultMode mode) throws SQLException {
        record.assertNotReadOnly();

        execute(getLanguage().delete(mode, record));
    }

    /**
     * Deletes multiple records by exeuting a DELETE FROM table WHERE id IN (...)
     *
     * @param records List of records to delete (must be of the same class, and bound to the same Database)
     * @throws SQLException
     *             if a database access error occurs.
     */
    public void delete(Collection<? extends Record> records) throws SQLException {
        for (Record record : records) {
            record.assertNotReadOnly();
        }

        execute(getLanguage().delete(ResultMode.NO_RESULT, records));
    }

    /**
     * Inserts the record's changed values into the database by executing an SQL INSERT query.
     * The record's primary key value is set to the primary key generated by the database.
     *
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public void insert(Record record, ResultMode mode) throws SQLException {
        record.assertNotReadOnly();

        if (record.isStale()) {
            throw new IllegalStateException("Attempt to insert a stale record!");
        }

        execute(language.insert(mode, record));
    }

    public void insert(Record record) throws SQLException {
        insert(record, ResultMode.REPOPULATE);
    }

    public void insert(Collection<? extends Record> records) throws SQLException {
        insert(records, ResultMode.REPOPULATE);
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
     * @param isFullRepopulate Whether or not to fully re-populate the record columns, or just update their primary key value and markStale()
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public void insert(Collection<? extends Record> records, ResultMode mode) throws SQLException {
        if (records.isEmpty()) {
            return;
        }
        execute(language.insert(mode, records));
    }

    private void execute(Iterator<Batch> iterator) throws SQLException {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        Language language = getLanguage();

        while (iterator.hasNext()) {
            Batch batch = iterator.next();
            ResultMode mode = batch.getResultMode();
            Table table = batch.getTable();
            Query query = batch.getQuery();
            List<Record> records = batch.getRecords();

            try {
                Map<Object, Record> map = null;

                if (mode != ResultMode.NO_RESULT) {
                    preparedStatement = prepare(query);
                    preparedStatement.execute();
                    continue;
                } else if (language.isReturningSupported()) {
                    preparedStatement = prepare(query);
                    resultSet = preparedStatement.executeQuery();
                } else {
                    preparedStatement = prepare(query, true);
                    preparedStatement.execute();
                    resultSet = preparedStatement.getGeneratedKeys();
                    map = new HashMap<Object, Record>();
                }

                RecordIterator recordIterator = null;
                for (Record record : records) {
                    if (!resultSet.next()) {
                        throw new IllegalStateException(String.format("Too few rows returned? Expected %d rows from query %s", records.size(), query.getSql()));
                    }
                    if (language.isReturningSupported() && mode != ResultMode.NO_RESULT) {
                        // RETURNING rocks!
                        if (recordIterator == null) {
                            recordIterator = new RecordIterator(this, resultSet);
                            recordIterator.setCascadingClose(false);
                        }
                        recordIterator.populate(record);
                    } else {
                        Column column = record.getOrCreateColumn(table.getPrimaryKey().getSymbol());
                        column.setValue(resultSet.getObject(1));
                        column.setChanged(false);
                        map.put(column.getValue(), record);
                        record.stale(false);    // actually still stale but need to repopulate
                    }
                }

                if (map != null) {
                    resultSet.close();
                    resultSet = null;
                    preparedStatement.close();
                    preparedStatement = null;

                    // records must not be stale, or Query will generate SELECTs
                    Query q = getLanguage().buildSelectQuery(table, table.getPrimaryKey(), records);

                    preparedStatement = prepare(q);
                    resultSet = preparedStatement.executeQuery();
                    recordIterator = new RecordIterator(this, resultSet);
                    recordIterator.setCascadingClose(false);

                    int idColumn = resultSet.findColumn(table.getPrimaryKey().getSymbol().getName());
                    while (resultSet.next()) {
                        recordIterator.populate(map.get(resultSet.getObject(idColumn)));
                    }
                }

            } catch (SQLException sqlException) {
                // records are in an unknown state, mark them stale
                for (Record record : records) {
                    record.stale(true);
                }
                throw language.rethrow(sqlException);
            } finally {
                try {
                    if (resultSet != null) {
                        resultSet.close();
                    }
                } finally {
                    if (preparedStatement != null) {
                        preparedStatement.close();
                    }
                }
            }
        }
    }

    /**
     * Updates the record's changed column values by executing an SQL UPDATE query.
     *
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public int update(Record record, ResultMode mode, Composite key) throws SQLException {
        int rowsUpdated = 0;

        record.assertNotReadOnly();

        if (!record.isChanged()) {
            return rowsUpdated;
        }

        if (record.isStale()) {
            throw new IllegalStateException("Attempt to update a stale record!");
        }

        if (record.isCompositeKeyNull(key)) {
            throw new IllegalStateException("Primary/unique key contains NULL value(s)");
        }

        execute(getLanguage().update(mode, record));

        return rowsUpdated;
    }

    public int update(Record record, Composite key) throws SQLException {
        return update(record, ResultMode.REPOPULATE, record.primaryKey());
    }

    public int update(Record record, ResultMode mode) throws SQLException {
        return update(record, mode, record.primaryKey());
    }

    public int update(Record record) throws SQLException {
        return update(record, ResultMode.REPOPULATE);
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
    public void update(Collection<? extends Record> records) throws SQLException {
        update(records, ResultMode.REPOPULATE);
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
    public void update(Collection<? extends Record> records, ResultMode mode) throws SQLException {
        update(records, mode, null);
    }

    /**
     * Executes a batch UPDATE (UPDATE ... SET x = s.x, y = s.y FROM (values, ...) s WHERE id = s.id).
     *
     * For large sets of records, the use of chunkSize is recommended to avoid out-of-memory errors and too long SQL queries.
     *
     * Currently, this is only supported on PostgreSQL. The method will fall back to using individual update()s on other databases.
     *
     * @param records List of records to insert (must be of the same class, and bound to the same Database)
     * @param size Splits the records into chunks, <= 0 disables
     * @param mode
     * @param composite composite key
     * @throws SQLException
     *             if a database access error occurs
     */
    public void update(Collection<? extends Record> records, ResultMode mode, Composite composite) throws SQLException {
        execute(getLanguage().update(mode, composite, records));
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
    public void refresh(Record record) {
        if (record.isStale()) {
            try {
                record.assertPrimaryKeyNotNull();
                populateById(record, record.primaryKey().valueFrom(record, true));
            } catch (SQLException e) {
                throw new RuntimeException("Failed to refresh stale record", e);
            }
            record.stale(false);
        }
    }
}
