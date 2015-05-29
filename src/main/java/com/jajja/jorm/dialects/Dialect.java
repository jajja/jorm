package com.jajja.jorm.dialects;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.jajja.jorm.Composite;
import com.jajja.jorm.Query;
import com.jajja.jorm.Record;
import com.jajja.jorm.Record.ResultMode;
import com.jajja.jorm.Row.Column;
import com.jajja.jorm.Symbol;
import com.jajja.jorm.Table;
import com.jajja.jorm.Composite.Value;
import com.jajja.jorm.exceptions.CheckViolationException;
import com.jajja.jorm.exceptions.DeadlockDetectedException;
import com.jajja.jorm.exceptions.ForeignKeyViolationException;
import com.jajja.jorm.exceptions.JormSqlException;
import com.jajja.jorm.exceptions.LockTimeoutException;
import com.jajja.jorm.exceptions.UniqueViolationException;

public abstract class Dialect {
    private final String extraNameChars;
    private final String identifierQuoteString;
    private final EnumSet<Feature> features = EnumSet.noneOf(Feature.class);

    public static enum Feature {
        BATCH_INSERTS,
        BATCH_UPDATES,
        ROW_WISE_COMPARISONS     // WHERE (id, name, ...) = (1, 'foo', ...) and (id, name) IN ((1, 'foo'), (2, 'bar'))
    }

    public static enum ExceptionType {
        UNKNOWN,
        FOREIGN_KEY_VIOLATION,
        UNIQUE_VIOLATION,
        CHECK_VIOLATION,
        DEADLOCK_DETECTED,
        LOCK_TIMEOUT;
    }

    public static enum DatabaseProduct {
        UNKNOWN(0, "Unknown"),
        POSTGRESQL(1, "PostgreSQL"),
        SQL_SERVER(2, "Microsoft SQL Server"),
        MYSQL(3, "MySQL"),
        ORACLE(4, "Oracle");

        private Integer id;
        private String name;
        private static Map<String, DatabaseProduct> nameMap = new HashMap<String, DatabaseProduct>();

        static {
            for (DatabaseProduct databaseProduct : DatabaseProduct.values()) {
                nameMap.put(databaseProduct.getName(), databaseProduct);
            }
        }

        DatabaseProduct(int id, String name) {
            this.id = id;
            this.name = name;
        }

        public int getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public static DatabaseProduct getByName(String databaseProductName) {
            DatabaseProduct dp = nameMap.get(databaseProductName);
            return dp != null ? dp : UNKNOWN;
        }
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
    protected Dialect(String database, Connection connection) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        String identifierQuoteString = metaData.getIdentifierQuoteString();
        if (" ".equals(identifierQuoteString)) {
            identifierQuoteString = null;
        }
        this.identifierQuoteString = identifierQuoteString;
        this.extraNameChars = metaData.getExtraNameCharacters();
    }

    public static Dialect get(String database, Connection connection) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        String databaseProductName = metaData.getDatabaseProductName();

        switch (DatabaseProduct.getByName(databaseProductName)) {
        case MYSQL:
            return new MysqlDialect(database, connection);
        case POSTGRESQL:
            return new PostgresqlDialect(database, connection);
        case SQL_SERVER:
            return new SqlServerDialect(database, connection);
        default:
            return new GenericDialect(database, connection);
        }
    }

    protected void feature(Feature feature) {
        features.add(feature);
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
    public boolean supports(Feature feature) {
        return features.contains(feature);
    }

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

//    /**
//     * SQL exception predicate for foreign key violation.
//     *
//     * @param sqlException
//     *            the SQL exception to evaluate.
//     * @return true if the SQL exception can be identified as a foreign key
//     *         violation, false otherwise.
//     */
//    public boolean isForeignKeyViolation(SQLException sqlException) {
//        return ExceptionType.FOREIGN_KEY_VIOLATION.equals(getExceptionType(sqlException));
//    }
//
//    /**
//     * SQL exception predicate for unique violation.
//     *
//     * @param sqlException
//     *            the SQL exception to evaluate.
//     * @return true if the SQL exception can be identified as a unique
//     *         violation.
//     */
//    public boolean isUniqueViolation(SQLException sqlException) {
//        return ExceptionType.UNIQUE_VIOLATION.equals(getExceptionType(sqlException));
//    }
//
//    /**
//     * SQL exception predicate for check violation.
//     *
//     * @param sqlException
//     *            the SQL exception to evaluate.
//     * @return true if the SQL exception can be identified as a check violation,
//     *         false otherwise.
//     */
//    public boolean isCheckViolation(SQLException sqlException) {
//        return ExceptionType.CHECK_VIOLATION.equals(getExceptionType(sqlException));
//    }
//
//    /**
//     * SQL exception predicate for foreign key detected deadlock.
//     *
//     * @param sqlException
//     *            the SQL exception to evaluate.
//     * @return true if the SQL exception can be identified as a detected
//     *         deadlock, false otherwise.
//     */
//    public boolean isDeadlockDetected(SQLException sqlException) {
//        return ExceptionType.DEADLOCK_DETECTED.equals(getExceptionType(sqlException));
//    }
//
//    /**
//     * SQL exception predicate for lock timeout.
//     *
//     * @param sqlException
//     *            the SQL exception to evaluate.
//     * @return true if the SQL exception can be identified as a lock timeout,
//     *         false otherwise.
//     */
//    public boolean isLockTimeout(SQLException sqlException) {
//        return ExceptionType.LOCK_TIMEOUT.equals(getExceptionType(sqlException));
//    }

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
        if (sqlException instanceof JormSqlException) {
            throw (JormSqlException)sqlException;
        }

        switch (getExceptionType(sqlException)) {
        case CHECK_VIOLATION:
            throw new CheckViolationException(sql, sqlException);
        case DEADLOCK_DETECTED:
            throw new DeadlockDetectedException(sql, sqlException);
        case FOREIGN_KEY_VIOLATION:
            throw new ForeignKeyViolationException(sql, sqlException);
        case LOCK_TIMEOUT:
            throw new LockTimeoutException(sql, sqlException);
        case UNIQUE_VIOLATION:
            throw new UniqueViolationException(sql, sqlException);
        default:
            throw new JormSqlException(sql, sqlException);
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

    @Deprecated
    private Query getQuery() {
        throw new IllegalStateException("Class is pending deletion!");
    }

    public Query toSqlExpression(Value value) {
        Query query = getQuery() ;
        Symbol[] columns = value.getComposite().getSymbols();
        Object[] values = value.getValues();
        boolean isFirst = true;
        for (int i = 0; i < columns.length; i++) {
            if (isFirst) {
                isFirst = false;
            } else {
                query.append(" AND ");
            }
            if (values[i] == null) {
                query.append("#:1# IS NULL", columns[i]);
            } else {
                query.append("#:1# = #2#", columns[i], values[i]);
            }
        }
        return query;
    }

    @SuppressWarnings("static-method")
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

    @SuppressWarnings("static-method")
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

    public Query buildSelectQuery(Table table, Composite.Value value) {
        Query query = getQuery() ;
        query.append("SELECT * FROM #1#", table);
        if (value != null) {
            query.append(" WHERE #1#", value);
        }
        return query;
    }

    public Query buildSelectQuery(Table table, Composite key, Collection<?> values) {
        Query query = getQuery() ;
        if (key.isSingle()) {
            query.append("SELECT * FROM #1# WHERE #2# IN (#3#)", table, key, values);
        } else if (supports(Feature.ROW_WISE_COMPARISONS)) {
            query.append("SELECT * FROM #1# WHERE (#2#) IN (", table, key);
            boolean isFirst = true;
            for (Object value : values) {
                if (isFirst) {
                    isFirst = false;
                } else {
                    query.append(", ");
                }
                query.append("(#1#)", value);
            }
            query.append(")");
        } else {
            throw new IllegalArgumentException("Row wise comparision not supported by the underlying database");
        }
        return query;
    }

    public Query buildDeleteQuery(Table table, Composite.Value value) {
        Query query = getQuery() ;
        query.append("DELETE FROM #1#", table);
        if (value != null) {
            query.append(" WHERE #1#", value);
        }
        return query;
    }

    public Query buildSingleInsertQuery(Record record, ResultMode mode) {
        Collection<Record> records = new ArrayList<Record>(1);
        records.add(record);
        return buildMultipleInsertQuery(records, mode);
    }

    public Query buildSingleUpdateQuery(Record record, ResultMode mode, Composite key) {
        Query query = getQuery() ;
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

    public Query buildMultipleInsertQuery(Collection<Record> records, ResultMode mode) {
        Query query = getQuery() ;

        Record template = null;
        Set<Symbol> symbols = new HashSet<Symbol>();

        for (Record record : records) {
            if (template == null) {
                template = record;
            } else if (record.getClass() != template.getClass()) {
                throw new IllegalArgumentException("All records must be of the same class");
            }
            for (Entry<Symbol, Column> e : record.columns().entrySet()) {
                if (e.getValue().isChanged()) {
                    symbols.add(e.getKey());
                }
            }
        }

        if (symbols.isEmpty()) {
            for (Symbol symbol : template.primaryKey().getSymbols()) {
                symbols.add(symbol);
            }
        }

        query.append("INSERT INTO #1# (", template.table());

        boolean isFirst = true;
        for (Symbol symbol : symbols) {
            query.append(isFirst ? "#:1#" : ", #:1#", symbol);
            isFirst = false;
        }
        query.append(")");

        if (mode != ResultMode.NO_RESULT && getReturnSetSyntax() == ReturnSetSyntax.OUTPUT) {
            appendReturnSetOutput(query, template, mode);
        }

        query.append(" VALUES (");
        boolean isFirstRecord = true;
        for (Record record : records) {
            if (isFirstRecord) {
                isFirstRecord = false;
            } else {
                query.append(", ");
            }
            Map<Symbol, Column> columns = record.columns();
            boolean isFirstValue = true;
            for (Symbol symbol : symbols) {
                if (isFirstValue) {
                    isFirstValue = false;
                } else {
                    query.append(", ");
                }
                Column column = columns.get(symbol);
                if (column != null && column.isChanged()) {
                    if (column.getValue() instanceof Query) {
                        query.append("#1#", column.getValue());
                    } else {
                        query.append("#?1#", column.getValue());
                    }
                } else {
                    query.append("DEFAULT");
                }
            }
        }
        query.append(")");

        if (mode != ResultMode.NO_RESULT && getReturnSetSyntax() == ReturnSetSyntax.RETURNING) {
            appendReturnSetReturning(query, template, mode);
        }

        return query;
    }

}
