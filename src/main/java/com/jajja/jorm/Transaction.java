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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
    private static Log log = LogFactory.getLog(Transaction.class);
    private String database;
    private DataSource dataSource;
    private Dialect dialect;
    private Timestamp now;
    private Connection connection;
    private Table table;
    private boolean isDestroyed = false;
    private boolean isLoggingEnabled = false;

    private void tracelog(String message) {
        if (isLoggingEnabled) {
            StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
            for (int i = 1; i < stackTrace.length; i++) {
                StackTraceElement stackTraceElement = stackTrace[i];
                if (!stackTraceElement.getClassName().startsWith("com.jajja.jorm.")) {
                    log.info(stackTraceElement + ": " + message);
                    break;
                }
            }
        }
    }

    Transaction(DataSource dataSource, String database) {
        this.database = database;
        this.dataSource = dataSource;
        table = new Table(database);
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
    public Dialect getDialect() {
        if (dialect == null) {
            try {
                dialect = new Dialect(database, getConnection());
            } catch (SQLException sqlException) {
                throw new RuntimeException("Failed to get database connection", sqlException);
            }
        }
        return dialect;
    }

    /**
     * Enables or disables SQL query logging on this transaction.
     */
    public void setLoggingEnabled(boolean loggingEnabled) {
        this.isLoggingEnabled = loggingEnabled;
    }

    /**
     * Provides the start time of the current transaction. The result is cached
     * until the end of the transaction.
     *
     * @return the start time of the current transaction.
     * @throws RuntimeException
     *             if a database access error occurs.
     */
    public Timestamp now() {
        if (now != null) {
            return now;
        }

        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            Connection connection = getConnection();
            preparedStatement = connection.prepareStatement(getDialect().getNowQuery());
            resultSet = preparedStatement.executeQuery();
            resultSet.next();
            now = resultSet.getTimestamp(1);
        } catch (SQLException sqlException) {
            throw new RuntimeException("Failed to execute: " + getDialect().getNowQuery(), sqlException);
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
            } catch (SQLException exception) {
                throw new RuntimeException("Failed closing the result set", exception);
            } finally {
                if (preparedStatement != null) {
                    try {
                        preparedStatement.close();
                    } catch (SQLException exception) {
                        throw new RuntimeException("Failed closing the prepared statment", exception);
                    }
                }
            }
        }

        return now;
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
     */
    public void close() {
        if (connection != null) {
            try {
                tracelog("ROLLBACK");
                connection.rollback();
            } catch (SQLException e) {
                log.fatal("Failed to rollback transaction", e);
            }
            try {
                connection.close();
            } catch (SQLException e) {
                log.fatal("Failed to close connection", e);
            }
            now = null;
            dialect = null;
            connection = null;
        }
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
     *
     * @throws SQLException
     *             if a database access error occurs.
     */
    public void commit() throws SQLException {
        if (connection != null) {
            tracelog("COMMIT");
            getConnection().commit();
            close();
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
     * @param returnGeneratedKeys sets Statement.RETURN_GENERATED_KEYS iff true
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
    public PreparedStatement prepare(String sql, List<Object> params) throws SQLException {
        return prepare(sql, params, false);
    }

    /**
     * Provides a prepared statement for the query given by a JDBC SQL statement and applicable parameters.
     *
     * @param sql
     *            the JDBC SQL statement.
     * @param params
     *            the applicable parameters.
     * @param returnGeneratedKeys sets Statement.RETURN_GENERATED_KEYS iff true
     * @throws SQLException
     *             if a database access error occurs.
     */
    public PreparedStatement prepare(String sql, List<Object> params, boolean returnGeneratedKeys) throws SQLException {
        tracelog(sql);
        try {
            PreparedStatement preparedStatement = getConnection().prepareStatement(sql, returnGeneratedKeys ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS);
            if (params != null) {
                int p = 1;
                for (Object param : params) {
                    preparedStatement.setObject(p++, param);
                }
            }
            return preparedStatement;
        } catch (SQLException e) {
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
        execute(new Query(getDialect(), sql, params));
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
            getDialect().rethrow(sqlException, query.getSql());
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
        return executeUpdate(new Query(getDialect(), sql, params));
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
            throw getDialect().rethrow(sqlException, query.getSql());
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
    public Record select(String sql, Object... params) throws SQLException {
        return select(new Query(getDialect(), sql, params));
    }

    /**
     * Provides an anonymous read-only record, populated with the first result from the
     * given query.
     *
     * @param query
     *            the query.
     * @return the matched record or null for no match.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public Record select(Query query) throws SQLException {
        Select select = new Select(table);
        if (!select.selectInto(query)) {
            select = null;
        }
        return select;
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
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public List<Record> selectAll(String sql, Object... params) throws SQLException {
        return selectAll(new Query(getDialect(), sql, params));
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
    public List<Record> selectAll(Query query) throws SQLException {
        ResultSet resultSet = null;
        try {
            List<Record> records = new LinkedList<Record>();
            resultSet = prepare(query).executeQuery();
            while (resultSet.next()) {
                Select select = new Select(table);
                select.populate(resultSet);
                records.add(select);
            }
            return records;
        } catch (SQLException sqlException) {
            throw getDialect().rethrow(sqlException, query.getSql());
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }
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
    public Savepoint save() throws SQLException {
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
    public Savepoint save(String name) throws SQLException {
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
     * Anonymous transaction local record for read only queries.
     */
    private static class Select extends Record {
        public Select(Table table) {
            super(table);
        }
    }
}
