package com.jajja.jorm.dialects;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map.Entry;

import com.jajja.jorm.Composite;
import com.jajja.jorm.Composite.Value;
import com.jajja.jorm.Query;
import com.jajja.jorm.Record;
import com.jajja.jorm.Record.ResultMode;
import com.jajja.jorm.Row.Column;
import com.jajja.jorm.Symbol;
import com.jajja.jorm.exceptions.CheckViolationException;
import com.jajja.jorm.exceptions.DeadlockDetectedException;
import com.jajja.jorm.exceptions.ForeignKeyViolationException;
import com.jajja.jorm.exceptions.JormSqlException;
import com.jajja.jorm.exceptions.LockTimeoutException;
import com.jajja.jorm.exceptions.UniqueViolationException;

public abstract class Dialect {
    private String database;
    private final String extraNameChars;
    private final String identifierQuoteString;

    public static enum ExceptionType {
        UNKNOWN,
        FOREIGN_KEY_VIOLATION,
        UNIQUE_VIOLATION,
        CHECK_VIOLATION,
        DEADLOCK_DETECTED,
        LOCK_TIMEOUT;
    }

    public static enum DatabaseProduct {
        OTHER,
        POSTGRESQL,
        MYSQL,
        SQL_SERVER,
        ORACLE;
    }

    public static enum ReturnSetSyntax {
        NONE,
        RETURNING,  // INSERT INTO foo (x, y) VALUES (1, 2) RETURNING *
        OUTPUT;     // INSERT INTO foo OUTPUT INSERTED.* (x, y) VALUES (1, 2)
    }

    /**
     * Constructs dialect specific configuration according a given database and
     * corresponding connection.
     *
     * @param database
     *            the name of the database, used for enhancing SQL exceptions.
     * @param connection
     *            the connection.
     * @throws SQLException
     *             when the connection fail.
     */
    Dialect(String database, Connection connection) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        String identifierQuoteString = metaData.getIdentifierQuoteString();
        if (" ".equals(identifierQuoteString)) {
            identifierQuoteString = null;
        }
        this.identifierQuoteString = identifierQuoteString;
        this.extraNameChars = metaData.getExtraNameCharacters();
        this.database = database;
    }

    public static Dialect get(String database, Connection connection) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        String databaseProductName = metaData.getDatabaseProductName();

        if ("PostgreSQL".equals(databaseProductName)) {
            return new PostgresqlDialect(database, connection);
        } else if ("Microsoft SQL Server".equals(databaseProductName)) {
            return new SqlServerDialect(database, connection);
        } else if ("MySQL".equals(databaseProductName)) {
            return new MysqlDialect(database, connection);
        //} else if ("Oracle".equals(databaseProductName)) {
        } else {
            return new GenericDialect(database, connection);
        }
    }

    /**
     * Returns the return set syntax supported by the database, or NONE if not supported.
     */
    public abstract ReturnSetSyntax getReturnSetSyntax();

    /**
     * Determines if the SQL dialect has support for row-wise comparison, i.e. WHERE (id, name, ...) = (1, 'foo', ...)
     *
     * @return true if the SQL dialect supports row-wise comparison, false otherwise.
     */
    public abstract boolean isRowWiseComparisonSupported();

    public Object getPrimaryKeyValue(ResultSet resultSet, int column) throws SQLException {
        return resultSet.getObject(column);
    }

    /**
     * <p>
     * Determines whether a given identifier contains characters that needs
     * escaped by known safe characters for the dialect, and thus if the
     * identifier has to be quoted in order to be well formed.
     * </p>
     * <p>
     * <strong>Note:</strong> that SQL keywords are not checked.
     * </p>
     *
     * @param string
     *            the identifier.
     * @return true if the identifier contains characters that needs to be
     *         escaped, false otherwise
     */
    public boolean isIdentifierQuotingRequired(String string) {
        for (int i = 0; i < string.length(); i++) {
            char ch = string.charAt(i);
            if ("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_".indexOf(ch) == -1 && extraNameChars.indexOf(ch) == -1) {
                return true;
            }
        }

        return false;
    }

    /**
     * Quotes an identifier such as a schema name, table name or column name.
     *
     * @param string
     *            the identifier.
     * @return the quoted identifier.
     * @throws RuntimeException
     *             if the identifier cannot be escaped.
     */
    public String quoteIdentifier(String string) {
        if (identifierQuoteString == null) {
            if (isIdentifierQuotingRequired(string)) {
                throw new RuntimeException("Unable to quote SQL identifier '" + string + "'; no quote string available and value contains characters that must be quoted");
            } else {
                return string;
            }
        }

        if (string.contains(identifierQuoteString)) {
            // getIdentifierQuoteString() does not specify how identifierQuoteString is supposed to be escaped ("\"", """", etc)
            throw new RuntimeException("Invalid SQL identifier: " + string);
        }

        StringBuilder quotedValue = new StringBuilder();
        quotedValue.append(identifierQuoteString);
        quotedValue.append(string);
        quotedValue.append(identifierQuoteString);

        return quotedValue.toString();
    }

    /**
     * SQL exception predicate for foreign key violation.
     *
     * @param sqlException
     *            the SQL exception to evaluate.
     * @return true if the SQL exception can be identified as a foreign key
     *         violation, false otherwise.
     */
    public boolean isForeignKeyViolation(SQLException sqlException) {
        return ExceptionType.FOREIGN_KEY_VIOLATION.equals(getExceptionType(sqlException));
    }

    /**
     * SQL exception predicate for unique violation.
     *
     * @param sqlException
     *            the SQL exception to evaluate.
     * @return true if the SQL exception can be identified as a unique
     *         violation.
     */
    public boolean isUniqueViolation(SQLException sqlException) {
        return ExceptionType.UNIQUE_VIOLATION.equals(getExceptionType(sqlException));
    }

    /**
     * SQL exception predicate for check violation.
     *
     * @param sqlException
     *            the SQL exception to evaluate.
     * @return true if the SQL exception can be identified as a check violation,
     *         false otherwise.
     */
    public boolean isCheckViolation(SQLException sqlException) {
        return ExceptionType.CHECK_VIOLATION.equals(getExceptionType(sqlException));
    }

    /**
     * SQL exception predicate for foreign key detected deadlock.
     *
     * @param sqlException
     *            the SQL exception to evaluate.
     * @return true if the SQL exception can be identified as a detected
     *         deadlock, false otherwise.
     */
    public boolean isDeadlockDetected(SQLException sqlException) {
        return ExceptionType.DEADLOCK_DETECTED.equals(getExceptionType(sqlException));
    }

    /**
     * SQL exception predicate for lock timeout.
     *
     * @param sqlException
     *            the SQL exception to evaluate.
     * @return true if the SQL exception can be identified as a lock timeout,
     *         false otherwise.
     */
    public boolean isLockTimeout(SQLException sqlException) {
        return ExceptionType.LOCK_TIMEOUT.equals(getExceptionType(sqlException));
    }

    /**
     * Classifies SQL exceptions by SQL states and error codes. In the current
     * configuration, PostgreSQL, MySQL and SQL Server exceptions can be classified.
     *
     * See {@link ExceptionType} for a list of exception types.
     *
     * If the type of exception is not known, UNKNOWN is returned.
     *
     * @param sqlException
     *            the exception to classify.
     * @return the exception type.
     */
    public abstract ExceptionType getExceptionType(SQLException sqlException);

    /**
     * Re-throws an SQLException as a {@link JormSqlException}. If the exception can be classified, it is
     * augmented further as either a {@link DeadlockDetectedException}, {@link ForeignKeyViolationException},
     * {@link LockTimeoutException} or {@link UniqueViolationException}.
     *
     * @param sqlException
     *            an original sqlException generated by a JDBC-implementation.
     * @param sql
     *            the SQL statement that generated the exception, or null if unknown/none.
     * @throws SQLException
     *             the possibly augmented SQL exception.
     */
    public JormSqlException rethrow(SQLException sqlException, String sql) throws SQLException {
        // XXX rewrite: switch() on getExceptionType()
        if (sqlException instanceof JormSqlException) {
            throw (JormSqlException) sqlException;
        } else if (isForeignKeyViolation(sqlException)) {
            throw new ForeignKeyViolationException(database, sql, sqlException);
        } else if (isUniqueViolation(sqlException)) {
            throw new UniqueViolationException(database, sql, sqlException);
        } else if (isCheckViolation(sqlException)) {
            throw new CheckViolationException(database, sql, sqlException);
        } else if (isLockTimeout(sqlException)) {
            throw new LockTimeoutException(database, sql, sqlException);
        } else if (isDeadlockDetected(sqlException)) {
            throw new DeadlockDetectedException(database, sql, sqlException);
        } else {
            throw new JormSqlException(database, sql, sqlException);
        }
    }

    /**
     * Re-throws an SQLException as a {@link JormSqlException}. If the exception can be classified, it is
     * augmented further as either a {@link DeadlockDetectedException}, {@link ForeignKeyViolationException},
     * {@link LockTimeoutException} or {@link UniqueViolationException}.
     *
     * @param sqlException
     *            an original sqlException generated by a JDBC-implementation.
     * @throws SQLException
     *             the possibly augmented SQL exception.
     */
    public JormSqlException rethrow(SQLException sqlException) throws SQLException {
        return rethrow(sqlException, null);
    }

    /**
     * Gets the dialect specific function call that returns the transaction start time,
     * eg. <tt>now()</tt>.
     *
     * @return the dialect specific function call for getting the transaction start time.
     */
    public abstract String getNowFunction();

    /**
     * Gets the dialect specific query that selects the transaction start time,
     * eg. <tt>SELECT now()</tt>.
     *
     * @return the dialect specific query for selecting transaction start time.
     */
    public abstract String getNowQuery();

    public Query toSqlExpression(Value value) {
        Query query = new Query(this);
        Symbol[] columns = value.getComposite().getSymbols();
        Object[] values = value.getValues();
        boolean isFirst = true;
        for (int i = 0; i < columns.length; i++) {
            if (isFirst) {
                isFirst = false;
                query.append("#:1# = #2#", columns[i], values[i]);
            } else {
                query.append(" AND #:1# = #2#", columns[i], values[i]);
            }
        }
        return query;
    }

    private void appendReturnSetOutput(Query query, Record record, ResultMode mode) {
        query.append(" OUTPUT");
        if (mode == ResultMode.ID_ONLY) {
            Symbol[] symbols = record.table().getPrimaryKey().getSymbols();
            for (int i = 0; i < symbols.length; i++) {
                query.append((i == 0) ? " INSERTED.#1#" : ", INSERTED.#1#", symbols[i]);
            }
        } else {
            query.append(" INSERTED.*");
        }
    }

    private void appendReturnSetReturning(Query query, Record record, ResultMode mode) {
        query.append(" RETURNING");
        if (mode == ResultMode.ID_ONLY) {
            Symbol[] symbols = record.table().getPrimaryKey().getSymbols();
            for (int i = 0; i < symbols.length; i++) {
                query.append((i == 0) ? " #1#" : ", #1#", symbols[i]);
            }
        } else {
            query.append(" *");
        }
    }

    public Query buildSingleInsertQuery(Record record, ResultMode mode) {
        Query query = new Query(this);

        query.append("INSERT INTO #1#", record.table());

        boolean isFirst = true;
        for (Entry<Symbol, Column> entry : record.columns().entrySet()) {
            Column column = entry.getValue();
            if (column.isChanged()) {
                query.append(isFirst ? "#:1#" : ", #:1#", entry.getKey());
                isFirst = false;
            }
        }

        if (isFirst) {
            // No columns are marked as changed, but we need to insert something...
            // "INSERT INTO foo DEFAULT VALUES" is not supported on all databases
            query.append("#1#)", record.primaryKey());
            if (mode != ResultMode.NO_RESULT && getReturnSetSyntax() == ReturnSetSyntax.OUTPUT) {
                appendReturnSetOutput(query, record, mode);
            }
            query.append(" VALUES");
            for (int i = 0; i < record.primaryKey().getSymbols().length; i++) {
                query.append(i == 0 ? " (DEFAULT" : ", DEFAULT");
            }
            query.append(")");
        } else {
            query.append(")");
            if (mode != ResultMode.NO_RESULT && getReturnSetSyntax() == ReturnSetSyntax.OUTPUT) {
                appendReturnSetOutput(query, record, mode);
            }
            query.append("VALUES (");
            isFirst = true;
            for (Entry<Symbol, Column> e : record.columns().entrySet()) {
                Column column = e.getValue();
                if (column.isChanged() && !record.table().isImmutable(e.getKey())) {
                    if (column.getValue() instanceof Query) {
                        query.append(isFirst ? "#1#" : ", #1#", column.getValue());
                    } else {
                        query.append(isFirst ? "#?1#" : ", #?1#", column.getValue());
                    }
                    isFirst = false;
                }
            }
            query.append(")");
        }

        if (mode != ResultMode.NO_RESULT && getReturnSetSyntax() == ReturnSetSyntax.RETURNING) {
            appendReturnSetReturning(query, record, mode);
        }

        return query;
    }

    public Query buildSingleUpdateQuery(Record record, ResultMode mode, Composite key) {
        Query query = new Query(this);
        query.append("UPDATE #1#", record.table());

        if (mode != ResultMode.NO_RESULT && getReturnSetSyntax() == ReturnSetSyntax.OUTPUT) {
            appendReturnSetOutput(query, record, mode);
        }

        boolean isFirst = true;
        for (Entry<Symbol, Column> entry : record.columns().entrySet()) {
            Column column = entry.getValue();
            if (column.isChanged()) {
                if (column.getValue() instanceof Query) {
                    query.append(isFirst ? " SET #:1# = #2#" : ", #:1# = #2#", entry.getKey(), column.getValue());
                } else {
                    query.append(isFirst ? " SET #:1# = #?2#" : ", #:1# = #?2#", entry.getKey(), column.getValue());
                }
                isFirst = false;
            }
        }

        query.append(" WHERE #1#", toSqlExpression(record.get(key)));

        if (mode != ResultMode.NO_RESULT && getReturnSetSyntax() == ReturnSetSyntax.RETURNING) {
            appendReturnSetReturning(query, record, mode);
        }

        return query;
    }
}
