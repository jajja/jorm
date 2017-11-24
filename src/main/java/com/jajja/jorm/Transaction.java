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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import javax.sql.DataSource;

import org.postgresql.util.PGobject;

import com.jajja.jorm.Composite.Value;
import com.jajja.jorm.Dialect.DatabaseProduct;
import com.jajja.jorm.Query.ParameterizedQuery;
import com.jajja.jorm.Record.ResultMode;
import com.jajja.jorm.RecordBatch.Slice;
import com.jajja.jorm.Row.Field;
import com.jajja.jorm.Row.NamedField;

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
 * @author Martin Korinth &lt;martin.korinth@jajja.com&gt;
 * @author Andreas Allerdahl &lt;andreas.allerdahl@jajja.com&gt;
 * @author Daniel Adolfsson &lt;daniel.adolfsson@jajja.com&gt;
 * @since 1.0.0
 */
public class Transaction {
    private final String database;
    private final DataSource dataSource;
    private Dialect dialect;
    private Connection connection;
    private boolean isClosed;
    private Calendar calendar;
    private Set<Listener> listeners;

    public static enum Event {
        OPEN,
        PREPARE,
        COMMIT,
        ROLLBACK,
        CLOSE;
    }

    public interface Listener {
        public void log(Transaction t, Event event, String sql, List<Object> params, List<StackTraceElement> stackTrace, Throwable exception);
    }

    public static class StdoutLogListener implements Listener {
        @Override
        public void log(Transaction t, Event event, String sql, List<Object> params, List<StackTraceElement> stackTrace, Throwable reason) {
            System.out.printf("[%d] %s: %s: %s (called from %s)\n", System.currentTimeMillis(), t.getDatabase(), event.name(), sql != null ? sql : "(no sql)", stackTrace.get(0));
            if (params != null) {
                int i = 1;
                for (Object param : params) {
                    System.out.printf("  SQL param #%d: %s\n", i++, param);
                }
            }
            System.out.printf("  Stack trace:\n");
            for (int i = 1; i < stackTrace.size() && i < 7; i++) {
                System.out.printf("    %s\n", stackTrace.get(i));
            }
            if (stackTrace.size() >= 7) {
                System.out.println("    ...");
            }
        }
    }

    private static List<StackTraceElement> getCallTrace() {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        List<StackTraceElement> list = new ArrayList<StackTraceElement>(stackTrace.length - 2);
        for (int i = 2; i < stackTrace.length; i++) {
            if (stackTrace[i].getClassName().startsWith("com.jajja.jorm.")) {
                continue;
            }
            list.add(stackTrace[i]);
        }
        return list;
    }

    private void tracelog(Event event, String sql, List<Object> params, Throwable reason) {
        if (listeners != null) {
            try {
                List<StackTraceElement> stackTrace = getCallTrace();
                for (Listener listener : listeners) {
                    try {
                        listener.log(this, event, sql, params, stackTrace, reason);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public boolean addListener(Listener listener) {
        if (listeners == null) {
            listeners = new LinkedHashSet<Listener>();
        }
        return listeners.add(listener);
    }

    public boolean removeListener(Listener listener) {
        if (listeners != null) {
            return listeners.remove(listener);
        }
        return false;
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

    protected Transaction(DataSource dataSource, String database, Calendar calendar) {
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
     * Provides the data source for the transaction.
     *
     * @return the data source.
     */
    public DataSource getDataSource() {
        return dataSource;
    }

    /**
     * Provides the SQL dialect of the connection for the transaction.
     */
    public Dialect getDialect() throws SQLException {
        getConnection();
        return dialect;
    }

    /**
     * Provides the start time of the current transaction. The result is cached
     * until the end of the transaction.
     *
     * @return the start time of the current transaction.
     * @throws RuntimeException
     *             if a database access error occurs.
     */
    public Timestamp now() throws SQLException {
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = prepare(getDialect().getNowQuery(), null);
            ResultSet resultSet = preparedStatement.executeQuery();
            resultSet.next();
            return resultSet.getTimestamp(1);
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
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
        if (isClosed) {
            throw new IllegalStateException("Connection is closed");
        }
        if (connection == null) {
            try {
                Connection c = dataSource.getConnection();
                c.setAutoCommit(false);
                dialect = new Dialect(database, c);
                connection = c;
                tracelog(Event.OPEN, null, null, null);
            } catch (SQLException e) {
                tracelog(Event.OPEN, null, null, e);
                throw e;
            }
        }
        return connection;
    }

    /**
     * Rolls back the current transaction and closes the database connection.
     * This is the equivalent of calling {@link #rollback()}
     * @throws
     */
    public void close() {
        if (!isClosed) {
            isClosed = true;

            try {
                if (connection != null) {
                    connection.close();
                }
                tracelog(Event.CLOSE, null, null, null);
            } catch (SQLException e) {
                tracelog(Event.CLOSE, null, null, e);
            } finally {
                dialect = null;
                connection = null;
            }
        }
    }

    /**
     * Commits the current transaction and closes the database connection.
     *
     * @throws SQLException
     *             if a database error occurs.
     */
    public void commit() throws SQLException {
        if (connection != null) {
            try {
                connection.commit();
                tracelog(Event.COMMIT, null, null, null);
            } catch (SQLException e) {
                tracelog(Event.COMMIT, null, null, e);
                throw e;
            }
        }
    }

    /**
     * Rolls back the current transaction and optionally closes the database connection.
     *
     * @throws SQLException
     *             if a database error occurs.
     */
    public void rollback() {
        if (connection != null) {
            try {
                connection.rollback();
                tracelog(Event.ROLLBACK, null, null, null);
            } catch (SQLException e) {
                tracelog(Event.ROLLBACK, null, null, e);
                //throw e;
            }
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
        ParameterizedQuery q = query.build(getDialect());
        return prepare(q.getSql(), q.getParameters(), returnGeneratedKeys);
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
    private PreparedStatement prepare(String sql, List<Object> params) throws SQLException {
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
    private PreparedStatement prepare(String sql, List<Object> params, boolean returnGeneratedKeys) throws SQLException {
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
            tracelog(Event.PREPARE, sql, params, null);
            return preparedStatement;
        } catch (SQLException e) {
            tracelog(Event.PREPARE, sql, params, e);
            throw getDialect().rethrow(e, sql);
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
        execute(new Query(sql, params));
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
        PreparedStatement preparedStatement = prepare(query);
        try {
            preparedStatement.execute();
        } catch (SQLException e) {
            throw getDialect().rethrow(e, query.buildFakeSql());
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
    public int executeUpdate(String sql, Object... params) throws SQLException {
        return executeUpdate(new Query(sql, params));
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
        PreparedStatement preparedStatement = prepare(query);
        try {
            return preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw getDialect().rethrow(e, query.buildFakeSql());
        } finally {
            preparedStatement.close();
        }
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
        return select(new Query(sql, params));
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
    public List<Row> selectAll(String sql, Object... params) throws SQLException {
        return selectAll(new Query(sql, params));
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
                iter = new RecordIterator(this, prepare(query));
                while (iter.next()) {
                    rows.add(iter.row());
                }
            } finally {
                if (iter != null) {
                    iter.close();
                }
            }
        } catch (SQLException e) {
            throw getDialect().rethrow(e, query.buildFakeSql());
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
        return new RecordIterator(this, prepare(query));
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
    public RecordIterator iterate(String sql, Object... params) throws SQLException {
        return iterate(new Query(sql, params));
    }

    /**
     * Deprecated: renamed to savepoint()
     *
     * Sets an unnamed savepoint on the active transaction. If the transaction
     * is dormant it begins and enters the active state.
     *
     * @return the savepoint.
     * @throws SQLException
     * @throws SQLException
     *             if a database access error occurs.
     */
    @Deprecated
    public Savepoint save() throws SQLException {
        return savepoint();
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
    @Deprecated
    public Savepoint save(String name) throws SQLException {
        return savepoint(name);
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
            throw getDialect().rethrow(e);
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
     * @param record
     *            the record.
     * @param value
     *            the value to match.
     * @return true if the record could be updated with a matching row from the
     *         table.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public boolean populateByCompositeValue(Record record, Value value) throws SQLException {
        return selectInto(record, getSelectQuery(record.getClass(), value));
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
        if (id instanceof Value) {
            return populateByCompositeValue(record, (Value)id);
        }
        return populateByCompositeValue(record, record.primaryKey().value(id));
    }

    private static <T extends Record> Query getSelectQuery(Class<T> clazz) throws SQLException {
        return new Query("SELECT * FROM #1# ", clazz);
    }

    private static <T extends Record> Query getDeleteQuery(Class<T> clazz) throws SQLException {
        return new Query("DELETE FROM #1# ", clazz);
    }

    public <T extends Record> Query getSelectQuery(Class<T> clazz, Object value) throws SQLException {
        Value v;
        if (value instanceof Value) {
            v = (Value)value;
        } else {
            v = Record.primaryKey(clazz).value(value);
        }
        Dialect dialect = getDialect();
        Query query = getSelectQuery(clazz);
        query.append("WHERE ");
        query.append(dialect.toSqlExpression(v));
        return query;
    }

    public <T extends Record> Query getDeleteQuery(Class<T> clazz, Object value) throws SQLException {
        Value v;
        if (value instanceof Value) {
            v = (Value)value;
        } else {
            v = Record.primaryKey(clazz).value(value);
        }
        Dialect dialect = getDialect();
        Query query = getDeleteQuery(clazz);
        query.append("WHERE ");
        query.append(dialect.toSqlExpression(v));
        return query;
    }

    /**
     * Builds a generic SQL query for the record. XXX redoc
     *
     * @deprecated Use Query constructor directly
     * @param sql
     *            the SQL statement to represent the query.
     * @return the built query.
     * @throws SQLException
     */
    @Deprecated
    public Query build() throws SQLException {
        return new Query();
    }

    /**
     * Builds a generic SQL query for the record.
     *
     * @deprecated Use Query constructor directly
     * @param sql
     *            the SQL statement to represent the query.
     * @return the built query.
     * @throws SQLException
     */
    @Deprecated
    public Query build(String sql) throws SQLException {
        return new Query(sql);
    }

    /**
     * Builds a generic SQL query for the record and quotes identifiers from the
     * given parameters according to the SQL dialect of the mapped database of
     * the record.
     *
     * @deprecated Use Query constructor directly
     * @param sql
     *            the Jorm SQL statement to represent the query.
     * @param params
     *            the parameters applying to the SQL hash markup.
     * @return the built query.
     * @throws SQLException
     */
    @Deprecated
    public Query build(String sql, Object... params) throws SQLException {
        return new Query(sql, params);
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
    public <T extends Record> T find(Class<T> clazz, Object value) throws SQLException {
        return select(clazz, getSelectQuery(clazz, value));
    }

    public int delete(Class<? extends Record> clazz, Object value) throws SQLException {
        return executeUpdate(getDeleteQuery(clazz, value));
    }

    /**
     * Provides a selected record from the mapped database table, populated with
     * the first result for which the column matches the value.
     *
     * @param clazz
     *            the class defining the table mapping.
     * @param column
     *            the column name
     * @param value
     *            the composite key value
     * @return the matched record or null for no match.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public <T extends Record> T find(Class<T> clazz, String column, Object value) throws SQLException {
        return select(clazz, getSelectQuery(clazz, new Composite(column).value(value)));
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
        return selectAll(clazz, getSelectQuery(clazz, value));
    }

    /**
     * Provides a complete list of selected records from the mapped database
     * table, populated with the results for which the column matches the value.
     *
     * @param clazz
     *            the class defining the table mapping.
     * @param composite
     *            the column name
     * @param value
     *            the composite key value
     * @return the matched records.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public <T extends Record> List<T> findAll(Class<T> clazz, String column, Object value) throws SQLException {
        return selectAll(clazz, getSelectQuery(clazz, new Composite(column).value(value)));
    }

    public <T extends Record> List<T> findAll(Class<T> clazz) throws SQLException {
        return selectAll(clazz, getSelectQuery(clazz));
    }

    public RecordIterator iterate(Class<? extends Record> clazz, Value value) throws SQLException {
        return selectIterator(clazz, getSelectQuery(clazz, value));
    }

    public RecordIterator iterate(Class<? extends Record> clazz) throws SQLException {
        return selectIterator(clazz, getSelectQuery(clazz));
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
    public <T extends Record> T findById(Class<T> clazz, Object id) throws SQLException {
        return find(clazz, id);
    }

    public int deleteById(Class<? extends Record> clazz, Object id) throws SQLException {
        return delete(clazz, Record.primaryKey(clazz).value(id));
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
        return select(clazz, new Query(sql));
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
        return select(clazz, new Query(sql, params));
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
        return selectAll(clazz, new Query(sql));
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
        return selectAll(clazz, new Query(sql, params));
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
                if (iter != null) {
                    iter.close();
                }
            }
        } catch (SQLException e) {
            throw getDialect().rethrow(e, query.buildFakeSql());
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
        return selectIterator(clazz, new Query(sql));
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
        return selectIterator(clazz, new Query(sql, params));
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
            throw getDialect().rethrow(e, query.buildFakeSql());
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
    public <T extends Row> Map<Composite.Value, T> selectIntoMap(Map<Composite.Value, T> map, Class<T> clazz, Object key, boolean allowDuplicates, Query query) throws SQLException {
        return selectIntoTypedMap(map, clazz, key, Composite.Value.class, allowDuplicates, query);
    }

    public <T extends Row> Map<Composite.Value, T> selectIntoMap(Map<Composite.Value, T> map, Class<T> clazz, Object key, boolean allowDuplicates, String sql, Object... params) throws SQLException {
        return selectIntoMap(map, clazz, key, allowDuplicates, new Query(sql, params));
    }

    @Deprecated
    public <T extends Record> Map<Composite.Value, T> selectIntoMap(Map<Composite.Value, T> map, Class<T> clazz, Object key, boolean allowDuplicates) throws SQLException {
        return selectIntoMap(map, clazz, key, allowDuplicates, getSelectQuery(clazz));
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
    public <T, C extends Row> Map<T, C> selectIntoTypedMap(Map<T, C> map, Class<C> clazz, Object key, Class<T> keyType, boolean allowDuplicates, Query query) throws SQLException {
        try {
            RecordIterator iter = null;
            Composite compositeKey = Composite.get(key);
            try {
                boolean wantRecords = Record.isRecordSubclass(clazz);
                boolean isCompositeValue = keyType == Composite.Value.class;
                iter = new RecordIterator(this, prepare(query));
                while (iter.next()) {
                    @SuppressWarnings("unchecked")
                    C row = (C)(wantRecords ? iter.record((Class<? extends Record>)clazz) : iter.row());
                    @SuppressWarnings("unchecked")
                    T value = isCompositeValue
                            ? (T)compositeKey.valueFrom(row)
                            : (T)compositeKey.valueFrom(row).getValue();
                    if (map.put(value, row) != null && !allowDuplicates) {
                        throw new IllegalStateException("Duplicate key " + value);
                    }
                }
            } finally {
                if (iter != null) {
                    iter.close();
                }
            }
        } catch (SQLException e) {
            throw getDialect().rethrow(e, query.buildFakeSql());
        }
        return map;
    }

    public <T, C extends Row> Map<T, C> selectIntoTypedMap(Map<T, C> map, Class<C> clazz, Object key, Class<T> keyType, boolean allowDuplicates, String sql, Object... params) throws SQLException {
        return selectIntoTypedMap(map, clazz, key, keyType, allowDuplicates, new Query(sql, params));
    }

    @Deprecated
    public <T, C extends Record> Map<T, C> selectIntoTypedMap(Map<T, C> map, Class<C> clazz, Object key, Class<T> keyType, boolean allowDuplicates) throws SQLException {
        return selectIntoTypedMap(map, clazz, key, keyType, allowDuplicates, getSelectQuery(clazz));
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
    public <T extends Row> void selectAllIntoMap(HashMap<Composite.Value, List<T>> map, Class<T> clazz, Object key, Query query) throws SQLException {
        selectAllIntoTypedMap(map, clazz, key, Composite.Value.class, query);
    }

    public <T extends Row> void selectAllIntoMap(HashMap<Composite.Value, List<T>> map, Class<T> clazz, Object key, String sql, Object... params) throws SQLException {
        selectAllIntoMap(map, clazz, key, new Query(sql, params));
    }

    @Deprecated
    public <T extends Record> void selectIntoMap(HashMap<Composite.Value, List<T>> map, Class<T> clazz, Object key) throws SQLException {
        selectAllIntoMap(map, clazz, key, getSelectQuery(clazz));
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
    public <T, C extends Row> void selectAllIntoTypedMap(HashMap<T, List<C>> map, Class<C> clazz, Object key, Class<T> keyType, Query query) throws SQLException {
        try {
            RecordIterator iter = null;
            Composite compositeKey = Composite.get(key);
            try {
                boolean wantRecords = Record.isRecordSubclass(clazz);
                boolean isCompositeValue = keyType == Composite.Value.class;
                iter = new RecordIterator(this, prepare(query));
                while (iter.next()) {
                    @SuppressWarnings("unchecked")
                    C row = (C)(wantRecords ? iter.record((Class<? extends Record>)clazz) : iter.row());
                    @SuppressWarnings("unchecked")
                    T value = isCompositeValue
                            ? (T)compositeKey.valueFrom(row)
                            : (T)compositeKey.valueFrom(row).getValue();
                    List<C> list = map.get(value);
                    if (list == null) {
                        list = new LinkedList<C>();
                        map.put(value, list);
                    }
                    list.add(row);
                }
            } finally {
                if (iter != null) {
                    iter.close();
                }
            }
        } catch (SQLException e) {
            throw getDialect().rethrow(e, query.buildFakeSql());
        }
    }

    public <T, C extends Row> void selectAllIntoTypedMap(HashMap<T, List<C>> map, Class<C> clazz, Object key, Class<T> keyType, String sql, Object... params) throws SQLException {
        selectAllIntoTypedMap(map, clazz, key, keyType, new Query(sql, params));
    }

    @Deprecated
    public <T, C extends Record> void selectIntoTypedMap(HashMap<T, List<C>> map, Class<C> clazz, Object key, Class<T> keyType) throws SQLException {
        selectAllIntoTypedMap(map, clazz, key, keyType, getSelectQuery(clazz));
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
    public <T extends Row> Map<Composite.Value, T> selectAsMap(Class<T> clazz, Object key, boolean allowDuplicates, Query query) throws SQLException {
        HashMap<Composite.Value, T> map = new HashMap<Composite.Value, T>();
        selectIntoMap(map, clazz, key, allowDuplicates, query);
        return map;
    }

    public <T extends Row> Map<Composite.Value, T> selectAsMap(Class<T> clazz, Object key, boolean allowDuplicates, String sql, Object... params) throws SQLException {
        return selectAsMap(clazz, key, allowDuplicates, new Query(sql, params));
    }

    @Deprecated
    public <T extends Record> Map<Composite.Value, T> selectAsMap(Class<T> clazz, Object key, boolean allowDuplicates) throws SQLException {
        return selectAsMap(clazz, key, allowDuplicates, getSelectQuery(clazz));
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
    public <T, C extends Row> Map<T, C> selectAsTypedMap(Class<C> clazz, Object key, Class<T> keyType, boolean allowDuplicates, Query query) throws SQLException {
        HashMap<T, C> map = new HashMap<T, C>();
        selectIntoTypedMap(map, clazz, key, keyType, allowDuplicates, query);
        return map;
    }

    public <T, C extends Row> Map<T, C> selectAsTypedMap(Class<C> clazz, Object key, Class<T> keyType, boolean allowDuplicates, String sql, Object... params) throws SQLException {
        return selectAsTypedMap(clazz, key, keyType, allowDuplicates, new Query(sql, params));
    }

    @Deprecated
    public <T, C extends Record> Map<T, C> selectAsTypedMap(Class<C> clazz, Object key, Class<T> keyType, boolean allowDuplicates) throws SQLException {
        return selectAsTypedMap(clazz, key, keyType, allowDuplicates, getSelectQuery(clazz));
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
    public <T extends Row> Map<Composite.Value, List<T>> selectAllAsMap(Class<T> clazz, Object key, Query query) throws SQLException {
        HashMap<Composite.Value, List<T>> map = new HashMap<Composite.Value, List<T>>();
        selectAllIntoMap(map, clazz, key, query);
        return map;
    }

    public <T extends Row> Map<Composite.Value, List<T>> selectAllAsMap(Class<T> clazz, Object key, String sql, Object... params) throws SQLException {
        return selectAllAsMap(clazz, key, new Query(sql, params));
    }

    @Deprecated
    public <T extends Record> Map<Composite.Value, List<T>> selectAllAsMap(Class<T> clazz, Object key) throws SQLException {
        return selectAllAsMap(clazz, key, getSelectQuery(clazz));
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
    public <T, C extends Row> Map<T, List<C>> selectAllAsTypedMap(Class<C> clazz, Object key, Class<T> keyType, Query query) throws SQLException {
        HashMap<T, List<C>> map = new HashMap<T, List<C>>();
        selectAllIntoTypedMap(map, clazz, key, keyType, query);
        return map;
    }

    public <T, C extends Row> Map<T, List<C>> selectAllAsTypedMap(Class<C> clazz, Object key, Class<T> keyType, String sql, Object... params) throws SQLException {
        return selectAllAsTypedMap(clazz, key, keyType, new Query(sql, params));
    }

    @Deprecated
    public <T, C extends Record> Map<T, List<C>> selectAllAsTypedMap(Class<C> clazz, Object key, Class<T> keyType) throws SQLException {
        return selectAllAsTypedMap(clazz, key, keyType, getSelectQuery(clazz));
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
        return selectInto(row, new Query(sql));
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
        return selectInto(row, new Query(sql, params));
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
                 if (iter != null) {
                    iter.close();
                }
            }
        } catch (SQLException e) {
            throw getDialect().rethrow(e, query.buildFakeSql());
        }
        return false;
    }

    /**
     * Populates all records in the given iterable of records with a single
     * prefetched reference of the given record class. Existing cached
     * references are not overwritten.
     *
     * @param records
     *            the records to populate with prefetched references.
     * @param foreignKeyColumn
     *            the column defining the foreign key to the referenced records.
     * @param clazz
     *            the class of the referenced records.
     * @param referredColumn
     *            the column defining the referred column of the referenced
     *            records.
     * @return the prefetched records.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public <T extends Record> Map<Composite.Value, T> prefetch(Iterable<? extends Row> rows, String foreignKeyColumn, Class<T> clazz, String referredColumn) throws SQLException {
        return prefetch(rows, foreignKeyColumn, clazz, referredColumn, false);
    }

    public <T extends Record> Map<Composite.Value, T> prefetch(Iterable<? extends Row> rows, String foreignKeyColumn, Class<T> clazz, String referredColumn, boolean ignoreInvalidReferences) throws SQLException {
        Set<Object> values = new HashSet<Object>();

        for (Row row : rows) {
            Field field = row.field(foreignKeyColumn);
            if (field != null && field.rawValue() != null && field.record() == null) {
                values.add(field.rawValue());
            }
        }

        if (values.isEmpty()) {
            return new HashMap<Composite.Value, T>();
        }

        final Composite key = new Composite(referredColumn);
        final Map<Composite.Value, T> map = new HashMap<Composite.Value, T>();
        Query q = null;
        int n = 0;
        for (Object value : values) {
            n++;
            if (q == null) {
                q = getSelectQuery(clazz).append("WHERE #:1# IN (#2#", referredColumn, value);
            } else {
                q.append(", #1#", value);
            }
            if (q.getParameters() >= 2048 || n == values.size()) { // MS SQL craps out at ~2500 parameters
                q.append(")");
                selectIntoMap(map, clazz, key, ignoreInvalidReferences, q);
                q = null;
            }
        }

        for (Row row : rows) {
            Field field = row.field(foreignKeyColumn);
            if (field != null && field.rawValue() != null && field.record() == null) {
                Record referenceRecord = map.get(key.value(field.rawValue()));
                if (referenceRecord == null && !ignoreInvalidReferences) {
                    throw new IllegalStateException(field.rawValue() + " not present in " + Table.get(clazz).getTable() + "." + referredColumn);
                }
                field.setValue(referenceRecord);
            }
        }

        return map;
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
    public void save(Iterable<? extends Record> records, int batchSize, ResultMode mode) throws SQLException {
        List<Record> insertRecords = new LinkedList<Record>();
        List<Record> updateRecords = new LinkedList<Record>();

        for (Record record : records) {
            if (record.isPrimaryKeyNullOrChanged()) {
                insertRecords.add(record);
            } else {
                updateRecords.add(record);
            }
        }

        insert(insertRecords, batchSize, mode);
        update(updateRecords, batchSize, mode);
    }

    /**
     * Batch saves the records. This is done by a call to {@link #insert()} if the id
     * column is null, unset or changed, otherwise by a call to {@link #update()}.
     *
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public void save(Iterable<? extends Record> records) throws SQLException {
        save(records, 0, ResultMode.REPOPULATE);
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
        record.assertNotReadOnly();
        Query query = new Query("DELETE FROM #1# WHERE #2#", record.table(), getDialect().toSqlExpression(record.id()));

        PreparedStatement preparedStatement = prepare(query);
        try {
            preparedStatement.execute();
        } finally {
            preparedStatement.close();
        }
        for (String column : record.primaryKey().getColumns()) {
            record.put(column, null);
        }
    }

    /**
     * Deletes multiple records by exeuting a DELETE FROM table WHERE id IN (...)
     *
     * @param records List of records to delete (must be of the same class, and bound to the same Database)
     * @throws SQLException
     *             if a database access error occurs.
     */
    public void delete(Slice<? extends Record> records, Composite primaryKey) throws SQLException {
        if (primaryKey == null) {
            primaryKey = records.primaryKey();
        }

        Query query = new Query("DELETE FROM #1# WHERE", records.clazz());
        Dialect dialect = getDialect();
        if (primaryKey.isSingle()) {
            query.append("#:1# IN (#2:@#)", primaryKey, records);
        } else {
            if (dialect.isRowWiseComparisonSupported()) {
                query.append(" (#:1#) IN (", primaryKey);
                boolean isFirst = true;
                for (Record record : records) {
                    query.append(isFirst ? "(#1#)" : ", (#1#)", record.id());
                    isFirst = false;
                }
                query.append(")");
            } else {
                boolean isFirst = true;
                for (Record record : records) {
                    query.append(isFirst ? " (#1#)" : " OR (#1#)", dialect.toSqlExpression(record.id()));
                    isFirst = false;
                }
            }
        }
        execute(query);
    }

    public <T extends Record> void delete(Iterable<T> records, int chunkSize, Composite primaryKey) throws SQLException {
        RecordBatch<T> batch = RecordBatch.of(records);
        if (chunkSize <= 0) {
            chunkSize = batch.size();
        }
        if (chunkSize == 0) {
            return;
        }
        for (Slice<T> slice : batch.slice(chunkSize)) {
            delete(slice, null);
        }
    }

    public void delete(Iterable<? extends Record> records, int chunkSize) throws SQLException {
        delete(records, chunkSize, null);
    }

    public void delete(Iterable<? extends Record> records) throws SQLException {
        delete(records, 0);
    }

    private void batchExecute(Query query, Slice<? extends Record> records, ResultMode mode) throws SQLException {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        Composite primaryKey = records.primaryKey();
        Dialect dialect = getDialect();

        // XXX UPDATE + REPOPULATE?
        if (mode != ResultMode.NO_RESULT && !primaryKey.isSingle() && !dialect.isReturningSupported()) {
            throw new UnsupportedOperationException("Batch operations on composite primary keys not supported by JDBC, and possibly your database (consider using ResultMode.NO_RESULT)");
        }

        try {
            boolean useReturning = (mode == ResultMode.REPOPULATE) && dialect.isReturningSupported();
            Map<Object, Record> map = null;

            if (useReturning) {
                query.append(" RETURNING *");   // XXX ID_ONLY support
                preparedStatement = prepare(query);
                resultSet = preparedStatement.executeQuery();
            } else {
                preparedStatement = prepare(query, true);
                preparedStatement.execute();
                resultSet = preparedStatement.getGeneratedKeys();
                if (mode == ResultMode.REPOPULATE) {
                    map = new HashMap<Object, Record>();
                } else if (mode == ResultMode.NO_RESULT) {
                    return;
                }
            }

            RecordIterator iter = null;
            for (Record record : records) {
                if (!resultSet.next()) {
                    throw new IllegalStateException("The getGeneratedKeys() result set contains too few rows. This is most likely a bug in your JDBC driver. Consider using ResultMode.NO_RESULT");
                }
                if (useReturning) {
                    if (iter == null) {
                        iter = new RecordIterator(this, resultSet);
                        iter.setCascadingClose(false);
                    }
                    iter.populate(record);
                } else {
                    Field field = record.getOrCreateField(primaryKey.getColumn());
                    field.setValue(resultSet.getObject(1));
                    field.setChanged(false);
                    if (mode == ResultMode.REPOPULATE) {
                        if (map == null) {
                            throw new IllegalStateException("bug");
                        }
                        map.put(field.dereference(), record);
                    }
                }
            }

            if (!useReturning && mode == ResultMode.REPOPULATE) {
                if (map == null) {
                    throw new IllegalStateException("bug");
                }

                resultSet.close();
                resultSet = null;
                preparedStatement.close();
                preparedStatement = null;

                Query q = getSelectQuery(records.clazz()).append("WHERE #:1# IN (#2:@#)", primaryKey.getColumn(), records);

                preparedStatement = prepare(q);
                resultSet = preparedStatement.executeQuery();
                iter = new RecordIterator(this, resultSet);
                iter.setCascadingClose(false);

                int idColumn = resultSet.findColumn(primaryKey.getColumn());
                if (Dialect.DatabaseProduct.MYSQL.equals(dialect.getDatabaseProduct())) {
                    while (iter.next()) {
                        iter.populate(map.get(resultSet.getLong(idColumn)));
                    }
                } else {
                    while (iter.next()) {
                        iter.populate(map.get(resultSet.getObject(idColumn)));
                    }
                }
            }
        } catch (SQLException sqlException) {
            throw dialect.rethrow(sqlException);
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

        if (mode != ResultMode.NO_RESULT && !record.primaryKey().isSingle() && !getDialect().isReturningSupported()) {
            throw new UnsupportedOperationException("INSERT with composite primary key not supported by JDBC, and possibly your database (consider using ResultMode.NO_RESULT)");
        }

        Query query = new Query();

        query.append("INSERT INTO #1# (", record.table());

        boolean isFirst = true;
        for (NamedField f : record.fields()) {
            if (f.field().isChanged() && !record.table().isImmutable(f.name())) {
                query.append(isFirst ? "#:1#" : ", #:1#", f.name());
                isFirst = false;
            }
        }

        if (isFirst) {
            // No columns are marked as changed, but we need to insert something... INSERT INTO foo DEFAULT VALUES is not supported on all databases
            query.append("#1#", record.primaryKey());
            for (int i = 0; i < record.primaryKey().getColumns().length; i++) {
                query.append(i == 0 ? ") VALUES (DEFAULT" : ", DEFAULT");
            }
        } else {
            query.append(") VALUES (");
            isFirst = true;
            for (NamedField f : record.fields()) {
                Field field = f.field();
                if (field.isChanged() && !record.table().isImmutable(f.name())) {
                    Object value = field.dereference();
                    if (value instanceof Query || value instanceof Composite.Value) {
                        query.append(isFirst ? "#1#" : ", #1#", value);
                    } else {
                        query.append(isFirst ? "#?1#" : ", #?1#", value);
                    }
                    isFirst = false;
                }
            }
        }
        query.append(")");

        if (mode == ResultMode.NO_RESULT) {
            execute(query);
            return;
        }

        if (getDialect().isReturningSupported()) {
            query.append(" RETURNING *");       // XXX ID_ONLY support
            selectInto(record, query);
        } else {
            PreparedStatement preparedStatement = prepare(query, true);
            ResultSet resultSet = null;
            Object id = null;
            try {
                preparedStatement.execute();
                resultSet = preparedStatement.getGeneratedKeys();
                if (resultSet.next()) {
                    id = resultSet.getObject(1);
                }
            } catch (SQLException e) {
                throw getDialect().rethrow(e, query.buildFakeSql());
            } finally {
                try {
                    if (resultSet != null) {
                        resultSet.close();
                    }
                } finally {
                    preparedStatement.close();
                }
            }

            if (id == null) {
                throw new RuntimeException("INSERT to " + record.table().toString() + " did not generate a key (AKA insert id): " + query.buildFakeSql());
            }
            Field field = record.getOrCreateField(record.primaryKey().getColumn());
            field.setValue(id);
            field.setChanged(false);
        }
    }

    public void insert(Record record) throws SQLException {
        insert(record, ResultMode.REPOPULATE);
    }

    /**
     * Executes a batch INSERT (INSERT INTO ... (columns...) VALUES (row1), (row2), (row3), ...) and repopulates the list with stored entities.
     *
     * @param records List of records to insert (must be of the same class, and bound to the same Database)
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public void insert(Iterable<? extends Record> records, ResultMode mode) throws SQLException {
        insert(records, 0, mode);
    }

    public void insert(Iterable<? extends Record> records) throws SQLException {
        insert(records, 0, ResultMode.REPOPULATE);
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
     * @param mode Result mode
     * @throws SQLException
     *             if a database access error occurs
     */
    public <T extends Record> void insert(Iterable<T> records, int chunkSize, ResultMode mode) throws SQLException {
        RecordBatch<T> batch = RecordBatch.of(records);

        if (chunkSize <= 0) {
            chunkSize = batch.size();
        }
        if (chunkSize == 0) {
            return;
        }
        for (Slice<T> slice : batch.slice(chunkSize)) {
            insert(slice, mode);
        }
    }

    public void insert(Slice<? extends Record> records, ResultMode mode) throws SQLException {
        Table table = records.table();
        Set<String> columns = records.dirtyColumns();
        Query query = new Query();

        if (columns.isEmpty()) {
            // No dirty columns?! We have to insert something...
            if (dialect.getDatabaseProduct() != DatabaseProduct.SQL_SERVER) {
                // Pick a random insert the primary key columns (id = DEFAULT), (id = DEFAULT), ...
                columns = new HashSet<String>(Arrays.asList(table.getPrimaryKey().getColumns()));
            } else {
                // ... unless we're dealing with MSSQL
                // INSERT INTO table () VALUES (), (), (); = syntax error.
                // INSERT INTO table (id) VALUES (DEFAULT), (DEFAULT), (DEFAULT); = DEFAULT or NULL are not allowed as explicit identity values
                // INSERT INTO table DEFAULT VALUES, DEFAULT VALUES, DEFAULT VALUES; = syntax error.
                columns = records.columns();
                columns.removeAll(Arrays.asList(table.getPrimaryKey().getColumns()));
                //if (columns.isEmpty()) { ...
            }
            // Remove all but one column
            for (Iterator<String> i = columns.iterator(); columns.size() > 1 && i.hasNext(); i.remove()) {
                ;
            }
        }

        query.append("INSERT INTO #1# (", table);

        boolean isFirst = true;
        for (String column : columns) {
            query.append(isFirst ? "#:1#" : ", #:1#", column);
            isFirst = false;
        }
        if (isFirst) {
            throw new RuntimeException("zero columns to insert!");
        }
        query.append(") VALUES ");

        isFirst = true;
        for (Record record : records) {
            query.append(isFirst ? "(" : ", (");
            isFirst = false;

            boolean isColumnFirst = true;
            for (String column : columns) {
                if (record.isDirty(column)) {
                    Object value = record.get(column);
                    if (value instanceof Query || value instanceof Composite.Value) {
                        query.append(isColumnFirst ? "#1#" : ", #1#", value);
                    } else {
                        query.append(isColumnFirst ? "#?1#" : ", #?1#", value);
                    }
                } else {
                    query.append(isColumnFirst ? "DEFAULT" : ", DEFAULT");
                }
                isColumnFirst = false;
            }
            query.append(")");
        }

        batchExecute(query, records, mode);
    }

    /**
     * Updates the record's changed column values by executing an SQL UPDATE query.
     *
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public int update(Record record, ResultMode mode, Composite primaryKey) throws SQLException {
        int rowsUpdated = 0;

        record.assertNotReadOnly();

        Query query = new Query();

        query.append("UPDATE #1# SET ", record.table());

        boolean isFirst = true;
        for (NamedField f : record.fields()) {
            Field field = f.field();
            if (field.isChanged()) {
                Object value = field.dereference();
                if (value instanceof Query || value instanceof Composite.Value) {
                    query.append(isFirst ? "#:1# = #2#" : ", #:1# = #2#", f.name(), value);
                } else {
                    query.append(isFirst ? "#:1# = #?2#" : ", #:1# = #?2#", f.name(), value);
                }
                isFirst = false;
            }
        }

        if (isFirst) {  // No columns dirty
            return 0;
        }

        if (record.isCompositeKeyNull(primaryKey)) {
            throw new IllegalStateException("Primary/unique key contains NULL value(s)");
        }

        query.append(" WHERE #1#", getDialect().toSqlExpression(record.get(primaryKey)));

        try {
            if (getDialect().isReturningSupported() && mode == ResultMode.REPOPULATE) {
                query.append(" RETURNING *");
                rowsUpdated = selectInto(record, query) ? 1 : 0;    // FIXME 1 row is not necessarily correct
            } else {
                rowsUpdated = executeUpdate(query);
            }
        } catch (SQLException e) {
            throw(e);
        }

        return rowsUpdated;
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
    public void update(Iterable<? extends Record> records) throws SQLException {
        update(records, 0, ResultMode.REPOPULATE);
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
    public void update(Iterable<? extends Record> records, int chunkSize, ResultMode mode) throws SQLException {
        update(records, chunkSize, mode, null);
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
    public <T extends Record> void update(Iterable<T> records, int chunkSize, ResultMode mode, Composite primaryKey) throws SQLException {
        Dialect dialect = getDialect();
        if (!Dialect.DatabaseProduct.POSTGRESQL.equals(dialect.getDatabaseProduct())) {
            for (Record record : records) {
                update(record, mode, primaryKey != null ? primaryKey : record.primaryKey());
            }
            return;
        }

        RecordBatch<T> batch = RecordBatch.of(records);
        if (chunkSize <= 0) {
            chunkSize = batch.size();
        }
        if (chunkSize == 0) {
            return;
        }
        for (Slice<T> slice : batch.slice(chunkSize)) {
            update(slice, mode, primaryKey);
        }
    }

    private static boolean havePGobject = false;
    static {
        try {
            Class.forName("org.postgresql.util.PGobject");
            havePGobject = true;
        } catch (ClassNotFoundException e) {
        }
    }

    private static String getPgDataType(Object v) {
        if (v instanceof java.sql.Timestamp) {
            return "timestamp";
        }
        if (v instanceof java.util.Date) {
            return "date";
        }
        if (havePGobject && v instanceof PGobject) {
            return ((PGobject)v).getType();
        }
        return null;
    }

    public void update(Slice<? extends Record> records, ResultMode mode, Composite primaryKey) throws SQLException {
        if (records.dirtyColumns().isEmpty()) {
            return;
        }

        if (primaryKey == null) {
            primaryKey = records.primaryKey();
        }

        Table table = records.table();
        Query query = new Query();
        String virtTable = table.getTable().equals("v") ? "v2" : "v";

        query.append("UPDATE #1# SET ", table);
        boolean isFirstColumn = true;
        for (String column : records.dirtyColumns()) {
            query.append(isFirstColumn ? "#:1# = #!2#.#:1#" : ", #:1# = #!2#.#:1#", column, virtTable);
            isFirstColumn = false;
        }

        query.append(" FROM (VALUES ");

        Set<String> virtTableColumns = new HashSet<String>(records.dirtyColumns());
        virtTableColumns.addAll(Arrays.asList(primaryKey.getColumns()));

        boolean isFirstValue = true;
        for (Record record : records) {
            if (record.isCompositeKeyNull(primaryKey)) {
                throw new IllegalArgumentException("Record has unset or NULL primary key: " + record);
            }
            isFirstColumn = true;
            query.append(isFirstValue ? "(" : ", (");
            for (String column : virtTableColumns) {
                Object value = record.get(column);
                if (value instanceof Query || value instanceof Composite.Value) {
                    query.append(isFirstColumn ? "#1#" : ", #1#", value);
                } else {
                    String pgDataType = getPgDataType(value);
                    if (pgDataType != null) {
                        query.append(isFirstColumn ? "cast(#?1# AS #:2#)" : ", cast(#?1# AS #:2#)", value, pgDataType);
                    } else {
                        query.append(isFirstColumn ? "#?1#" : ", #?1#", value);
                    }
                }
                isFirstColumn = false;
            }
            query.append(")");
            isFirstValue = false;
        }

        query.append(") #!1# (", virtTable);
        isFirstColumn = true;
        for (String column : virtTableColumns) {
            query.append(isFirstColumn ? "#:1#" : ", #:1#", column);
            isFirstColumn = false;
        }

        query.append(") WHERE");
        boolean isFirst = true;
        for (String column : primaryKey.getColumns()) {
            if (isFirst) {
                isFirst = false;
            } else {
                query.append(" AND");
            }
            query.append(" #:1#.#:2# = #:3#.#:2#", table, column, virtTable);
        }

        batchExecute(query, records, mode);
    }

    /**
     * Re-populates a record with fresh database values by a select query.
     *
     * @throws SQLException
     *             whenever a SQLException occurs.
     */
    public void refresh(Record record) throws SQLException {
        record.assertPrimaryKeyNotNull();
        populateById(record, record.primaryKey().valueFrom(record, true));
    }
}
