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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import com.jajja.jorm.Composite.Value;
import com.jajja.jorm.exceptions.CheckViolationException;
import com.jajja.jorm.exceptions.DeadlockDetectedException;
import com.jajja.jorm.exceptions.ForeignKeyViolationException;
import com.jajja.jorm.exceptions.JormSqlException;
import com.jajja.jorm.exceptions.LockTimeoutException;
import com.jajja.jorm.exceptions.UniqueViolationException;

/**
 * The implementation of dialect specific logic for SQL.
 *
 * @see Transaction
 * @author Martin Korinth <martin.korinth@jajja.com>
 * @author Andreas Allerdahl <andreas.allerdahl@jajja.com>
 * @since 1.0.0
 */
public class Dialect {
    private static final HashMap<DatabaseProduct, Info> infos = new HashMap<DatabaseProduct, Info>();
    private Info info;
    private boolean returningSupported; // TODO EnumSet?
    private boolean rowWiseComparison;
    private DatabaseProduct databaseProduct;
    private String extraNameChars;
    private String identifierQuoteString;
    private String database;

    private static class Info {
        private boolean useSqlState;
        private HashMap<Object, ExceptionType> errors = new HashMap<Object, ExceptionType>();
        private String nowFunction = "now()";
        private String nowQuery = "SELECT now()";

        public boolean useSqlState() {
            return useSqlState;
        }
        public void setUseSqlState() {
            this.useSqlState = true;
        }
        public void addError(Object error, ExceptionType type) {
            if (errors.containsKey(error)) {
                throw new IllegalStateException(error + " added twice");
            }
            errors.put(error, type);
        }
        public ExceptionType getError(Object key) {
            return errors.get(key);
        }
        public String getNowFunction() {
            return nowFunction;
        }
        public void setNowFunction(String nowFunction) {
            this.nowFunction = nowFunction;
        }
        public String getNowQuery() {
            return nowQuery;
        }
        public void setNowQuery(String nowQuery) {
            this.nowQuery = nowQuery;
        }
    }

    static {
        Info psql = new Info();
        psql.setUseSqlState();
        psql.addError("23503", ExceptionType.FOREIGN_KEY_VIOLATION);    // foreign_key_violation
        psql.addError("23505", ExceptionType.UNIQUE_VIOLATION);         // unique_violation
        psql.addError("23514", ExceptionType.CHECK_VIOLATION);         // check_violation
        psql.addError("40P01", ExceptionType.DEADLOCK_DETECTED);        // deadlock_detected
        psql.addError("55P03", ExceptionType.LOCK_TIMEOUT);             // lock_not_available
        infos.put(DatabaseProduct.POSTGRESQL, psql);

        Info mssql = new Info();
        //mssql.addError(547, ExceptionType.FOREIGN_KEY_VIOLATION);       // %ls statement conflicted with %ls %ls constraint '%.*ls'. The conflict occurred in database '%.*ls', table '%.*ls'%ls%.*ls%ls.
        mssql.addError(2601, ExceptionType.UNIQUE_VIOLATION);           // Cannot insert duplicate key row in object '%.*ls' with unique index '%.*ls'.
        mssql.addError(2627, ExceptionType.UNIQUE_VIOLATION);           // Violation of %ls constraint '%.*ls'. Cannot insert duplicate key in object '%.*ls'.
        mssql.addError(547, ExceptionType.CHECK_VIOLATION);             // %ls statement conflicted with %ls %ls constraint '%.*ls'. The conflict occurred in database '%.*ls', table '%.*ls'%ls%.*ls%ls.
        mssql.addError(1205, ExceptionType.DEADLOCK_DETECTED);          // Transaction (Process ID %d) was deadlocked on {%Z} resources with another process and has been chosen as the deadlock victim. Rerun the transaction.
        mssql.addError(1222, ExceptionType.LOCK_TIMEOUT);               // Lock request time out period exceeded.
        mssql.setNowFunction("getdate()");
        mssql.setNowQuery("SELECT getdate()");
        infos.put(DatabaseProduct.SQL_SERVER, mssql);

        Info mysql = new Info();
        mysql.addError(1216, ExceptionType.FOREIGN_KEY_VIOLATION);      // SQLSTATE: 23000 (ER_NO_REFERENCED_ROW) Cannot add or update a child row: a foreign key constraint fails
        mysql.addError(1217, ExceptionType.FOREIGN_KEY_VIOLATION);      // SQLSTATE: 23000 (ER_ROW_IS_REFERENCED) Cannot delete or update a parent row: a foreign key constraint fails
        mysql.addError(630, ExceptionType.UNIQUE_VIOLATION);            // Tuple already existed when attempting to insert
        mysql.addError(893, ExceptionType.UNIQUE_VIOLATION);            // Constraint violation e.g. duplicate value in unique index
        mysql.addError(1062, ExceptionType.UNIQUE_VIOLATION);           // SQLSTATE: 23000 (ER_DUP_ENTRY) Duplicate entry '%s' for key %d
        mysql.addError(1213, ExceptionType.DEADLOCK_DETECTED);          // SQLSTATE: 40001 (ER_LOCK_DEADLOCK) Deadlock found when trying to get lock; try restarting transaction
        mysql.addError(1205, ExceptionType.LOCK_TIMEOUT);               // SQLSTATE: HY000 (ER_LOCK_WAIT_TIMEOUT) Lock wait timeout exceeded; try restarting transaction
        infos.put(DatabaseProduct.MYSQL, mysql);
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

        this.database = database;
        databaseProduct = DatabaseProduct.getByName( metaData.getDatabaseProductName() );
        extraNameChars = metaData.getExtraNameCharacters();
        identifierQuoteString = metaData.getIdentifierQuoteString();
        if (" ".equals(identifierQuoteString)) {
            identifierQuoteString = null;
        }
        info = infos.get(databaseProduct);

        if (databaseProduct.equals(DatabaseProduct.POSTGRESQL)) {
            int major = metaData.getDatabaseMajorVersion();
            int minor = metaData.getDatabaseMinorVersion();
            returningSupported = major > 8 || (major == 8 && minor > 1);
            rowWiseComparison = true;
        }
    }

    /**
     * Determines if the SQL dialect has support for returning result set for
     * inserts and updates.
     *
     * @return true if the SQL the <tt>RETURNING</tt> clause for <tt>INSERT</tt>
     *         and <tt>UPDATE</tt> queries is supported by the database, false
     *         otherwise.
     */
    public boolean isReturningSupported() {
        return returningSupported;
    }

    /**
     * Determines if the SQL dialect has support for row-wise comparison, i.e. WHERE (id, name, ...) = (1, 'foo', ...)
     *
     * @return true if the SQL dialect supports row-wise comparison, false otherwise.
     */
    public boolean isRowWiseComparisonSupported() {
        return rowWiseComparison;
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

    //org.postgresql.core.Utils.appendEscapedLiteral()
    //com.mysql.jdbc.StringUtils.escapeQuote
    /*String brokenQuoteLiteral(String value) {
        value = value.replace("'", "''");
        if (DatabaseProduct.MYSQL.equals(databaseProduct)) {
            value = value.replace("\\", "\\\\");
        }
        return value;
    }*/

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
    public ExceptionType getExceptionType(SQLException sqlException) {
        if (info == null) return ExceptionType.UNKNOWN;

        Object key = info.useSqlState() ? sqlException.getSQLState() : sqlException.getErrorCode();
        ExceptionType error = info.getError(key);

        // XXX: separate check and unique for MS SQL SERVER
        //   The INSERT statement conflicted with the CHECK constraint
        //   The INSERT statement conflicted with the FOREIGN KEY constraint
        if (ExceptionType.CHECK_VIOLATION.equals(error) && DatabaseProduct.SQL_SERVER.equals(databaseProduct)) {
            if (sqlServerForeignKeyPattern.matcher(sqlException.getMessage()).matches()) {
                error = ExceptionType.FOREIGN_KEY_VIOLATION;
            }
        }

        return error != null ? error : ExceptionType.UNKNOWN;
    }
    private final static Pattern sqlServerForeignKeyPattern = Pattern.compile("^The [A-Z ]+ statement conflicted with the FOREIGN KEY constraint");

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
    public String getNowFunction() {
        return info.getNowFunction();
    }

    /**
     * Gets the dialect specific query that selects the transaction start time,
     * eg. <tt>SELECT now()</tt>.
     *
     * @return the dialect specific query for selecting transaction start time.
     */
    public String getNowQuery() {
        return info.getNowQuery();
    }

    /**
     * Provides the database product for the SQL dialect.
     *
     * @return the database product.
     */
    public DatabaseProduct getDatabaseProduct() {
        return databaseProduct;
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
            return nameMap.get(databaseProductName);
        }

    }

    public Query toSqlExpression(Composite composite, Value value) {
        Query query = new Query(this);
        Symbol[] columns = composite.getSymbols();
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
}
