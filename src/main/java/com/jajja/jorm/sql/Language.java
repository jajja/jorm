package com.jajja.jorm.sql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.jajja.jorm.Composite;
import com.jajja.jorm.Query;
import com.jajja.jorm.Record;
import com.jajja.jorm.Symbol;
import com.jajja.jorm.Table;
import com.jajja.jorm.Composite.Value;
import com.jajja.jorm.Record.ResultMode;
import com.jajja.jorm.Row.Column;
import com.jajja.jorm.exceptions.CheckViolationException;
import com.jajja.jorm.exceptions.DeadlockDetectedException;
import com.jajja.jorm.exceptions.ForeignKeyViolationException;
import com.jajja.jorm.exceptions.JormSqlException;
import com.jajja.jorm.exceptions.LockTimeoutException;
import com.jajja.jorm.exceptions.UniqueViolationException;

public abstract class Language {

    public static Language get(Connection connection) throws SQLException {
        Product product = new Product(connection);
        switch (product.server) {
        case MYSQL:
            return new Mysql(product);
        case POSTGRESQL:
            return new Postgres(product);
        case MICROSOFT_SQL_SERVER:
            return new Transact(product);
        case ORACLE:
            return new Oracle(product);
        default:
            return new Standard(product);
        }
    }

    private final Product product;

    protected Language(Product product) {
        this.product = product;
    }

    public abstract int getMaxParameters();
    public abstract Appender[] getAppenders(Operation operation);
    public abstract String getCurrentDateExpression();
    public abstract String getCurrentTimeExpression();
    public abstract String getCurrentDatetimeExpression();
    public abstract boolean isReturningSupported();
    public abstract boolean isBatchUpdateSupported();

    protected static enum Operation {
        INSERT,
        UPDATE,
        DELETE
    }

    protected static enum ExceptionType {
        UNKNOWN,
        FOREIGN_KEY_VIOLATION,
        UNIQUE_VIOLATION,
        CHECK_VIOLATION,
        DEADLOCK_DETECTED,
        LOCK_TIMEOUT;
    }

    protected static class Product {

        static enum Server {
            UNKNOWN(""),
            POSTGRESQL("PostgreSQL"),
            MICROSOFT_SQL_SERVER("Microsoft SQL Server"),
            MYSQL("MySQL"),
            ORACLE("Oracle");

            private String name;
            private static final Map<String, Server> NAMES = new HashMap<String, Server>();
            static {
                for (Server type : Server.values()) {
                    NAMES.put(type.getName(), type);
                }
            }

            Server(String name) {
                this.name = name;
            }

            public String getName() {
                return name;
            }

            public static Server get(String name) {
                Server type = NAMES.get(name);
                return type != null ? type : UNKNOWN;
            }

        }

        private final Server server;
        private final int major;
        private final int minor;
        private final String identifierQuoteString;
        private final String extraNameChars;

        public Product(Connection connection) throws SQLException {
            DatabaseMetaData metaData = connection.getMetaData();
            server = Server.get(metaData.getDatabaseProductName());
            major = metaData.getDatabaseMajorVersion();
            minor = metaData.getDatabaseMinorVersion();
            String identifierQuoteString = metaData.getIdentifierQuoteString();
            if (" ".equals(identifierQuoteString)) {
                identifierQuoteString = null;
            }
            this.identifierQuoteString = identifierQuoteString;
            this.extraNameChars = metaData.getExtraNameCharacters();
        }

        public Server getServer() {
            return server;
        }

        public int getMajor() {
            return major;
        }

        public int getMinor() {
            return minor;
        }

        public String getIdentifierQuoteString() {
            return identifierQuoteString;
        }

    }

    public static class Data {

        ResultMode resultMode;
        int parameters;
        Table table;
        Set<Symbol> pkSymbols;
        Set<Symbol> changedSymbols;
        List<Record> records;

        public Data(ResultMode resultMode) {
            this.resultMode = resultMode;
            parameters = 0;
            changedSymbols = new HashSet<Symbol>();
            records = new LinkedList<Record>();
        }

        public Data(ResultMode resultMode, int parameters, Set<Symbol> symbols) {
            this.resultMode = resultMode;
            this.parameters = parameters;
            this.changedSymbols = symbols;
            records = new LinkedList<Record>();
        }

        public void add(Data impression) {
            this.parameters += impression.parameters;
            this.changedSymbols.addAll(impression.changedSymbols);
            this.records.addAll(impression.records);
        }

        public void add(Composite composite, Record record) {
            if (table != null) {
                table = record.table();
                pkSymbols = new HashSet<Symbol>();
                if (composite == null) {
                    composite = table.getPrimaryKey();
                }
                for (Symbol symbol : composite.getSymbols()) {
                    pkSymbols.add(symbol);
                }
            } else if (table != record.table()) {
                throw new IllegalStateException(String.format("Mixed tables in batch! (%s != %s)", table, record.table()));
            }
            records.add(record);
        }

        public boolean isEmpty() {
            return records.isEmpty();
        }

        public boolean isScalar() {
            return records.size() == 1;
        }

    }

    public static class Batch {

        private final Query query;
        private final Data data;

        Batch(Data data, Query query) {
            this.data = data;
            this.query = query;
        }

        public ResultMode getResultMode() {
            return data.resultMode;
        }

        public Table getTable() {
            return data.table;
        }

        public List<Record> getRecords() {
            return data.records;
        }

        public Query getQuery() {
            return query;
        }

    }

    public static abstract class Appender {

        public abstract void append(Data data, Query query, ResultMode mode);

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
            if ("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_".indexOf(ch) == -1 && product.extraNameChars.indexOf(ch) == -1) {
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
        if (product.identifierQuoteString == null) {
            if (isIdentifierQuotingRequired(string)) {
                throw new RuntimeException("Unable to quote SQL identifier '" + string + "'; no quote string available and value contains characters that must be quoted");
            } else {
                return string;
            }
        }

        if (string.contains(product.identifierQuoteString)) {
            // getIdentifierQuoteString() does not specify how identifierQuoteString is supposed to be escaped ("\"", """", etc)
            throw new RuntimeException("Invalid SQL identifier: " + string);
        }

        StringBuilder quotedValue = new StringBuilder();
        quotedValue.append(product.identifierQuoteString);
        quotedValue.append(string);
        quotedValue.append(product.identifierQuoteString);

        return quotedValue.toString();
    }

    public Iterator<Batch> insert(ResultMode mode, Record ... records) {
        return batch(mode, Operation.INSERT, null, records);
    }

    public Iterator<Batch> update(ResultMode mode, Record ... records) {
        return batch(mode, Operation.UPDATE, null, records);
    }

    public Iterator<Batch> delete(ResultMode mode, Record ... records) {
        return batch(mode, Operation.DELETE, null, records);
    }

    public Iterator<Batch> insert(ResultMode mode, Composite composite, Record ... records) {
        return batch(mode, Operation.INSERT, composite, records);
    }

    public Iterator<Batch> update(ResultMode mode, Composite composite, Record ... records) {
        return batch(mode, Operation.UPDATE, composite, records);
    }

    public Iterator<Batch> delete(ResultMode mode, Composite composite, Record ... records) {
        return batch(mode, Operation.DELETE, composite, records);
    }

    public Iterator<Batch> insert(ResultMode mode, Collection<? extends Record> records) {
        return batch(mode, Operation.INSERT, null, records);
    }

    public Iterator<Batch> update(ResultMode mode, Collection<? extends Record> records) {
        return batch(mode, Operation.UPDATE, null, records);
    }

    public Iterator<Batch> delete(ResultMode mode, Collection<? extends Record> records) {
        return batch(mode, Operation.DELETE, null, records);
    }

    public Iterator<Batch> insert(ResultMode mode, Composite composite, Collection<? extends Record> records) {
        return batch(mode, Operation.INSERT, composite, records);
    }

    public Iterator<Batch> update(ResultMode mode, Composite composite, Collection<? extends Record> records) {
        return batch(mode, Operation.UPDATE, composite, records);
    }

    public Iterator<Batch> delete(ResultMode mode, Composite composite, Collection<? extends Record> records) {
        return batch(mode, Operation.DELETE, composite, records);
    }

    private Iterator<Batch> batch(ResultMode mode, Operation operation, Composite composite, Record ... records) {
        int size = 0;
        if (operation == Operation.UPDATE && !isBatchUpdateSupported()) {
            size = 1; // XXX: use structure to implement this
        }
        List<Batch> batches = new LinkedList<Batch>();
        Data data = new Data(mode);
        int i = 0;
        for (Record record : records) {
            Data increment = getData(record, operation, mode);
            if (data.parameters + increment.parameters < getMaxParameters() && (size < 1 || i < size)) {
                data.add(increment);
                i++;
            } else {
                batches.add(build(data, mode, operation));
                data = increment;
                i = 0;
            }
            data.add(composite, record);
        }
        if (!data.isEmpty()) {
            batches.add(build(data, mode, operation));
        }
        return batches.iterator();
    }

    private Iterator<Batch> batch(ResultMode mode, Operation operation, Composite composite, Collection<? extends Record> records) {
        int size = 0;
        if (operation == Operation.UPDATE && !isBatchUpdateSupported()) {
            size = 1; // XXX: use structure to implement this
        }
        List<Batch> batches = new LinkedList<Batch>();
        Data data = new Data(mode);
        int i = 0;
        for (Record record : records) {
            Data increment = getData(record, operation, mode);
            if (data.parameters + increment.parameters < getMaxParameters() && (size < 1 || i < size)) {
                data.add(increment);
                i++;
            } else {
                batches.add(build(data, mode, operation));
                data = increment;
                i = 0;
            }
            data.add(composite, record);
        }
        if (!data.isEmpty()) {
            batches.add(build(data, mode, operation));
        }
        return batches.iterator();
    }

    private Batch build(Data data, ResultMode mode, Operation operation) {
        Query query = new Query(this);
        for (Appender appender : getAppenders(operation)) {
            appender.append(data, query, mode);
        }
        return new Batch(data, query);
    }

    protected Data getData(Record record, Operation type, ResultMode mode) {
        switch(type) {
        case INSERT:
            return getInsertData(record, mode);
        case UPDATE:
            return getUpdateData(record, mode);
        case DELETE:
            return getDeleteData(record, mode);
        default:
            throw new IllegalStateException(String.format("The batch type %s is unknown!", type));
        }
    }

    protected Data getInsertData(Record record, ResultMode mode) {
        Set<Symbol> symbols = imprint(record);
        return new Data(mode, symbols.size(), symbols);
    }

    protected Data getUpdateData(Record record, ResultMode mode) {
        Set<Symbol> symbols = imprint(record);
        return new Data(mode, record.table().getPrimaryKey().size() + symbols.size(), symbols);
    }

    protected Data getDeleteData(Record record, ResultMode mode) {
        return new Data(mode, record.table().getPrimaryKey().size(), null);
    }

    private Set<Symbol> imprint(Record record) {
        Set<Symbol> symbols = new HashSet<Symbol>();
        for (Entry<Symbol, Column> e : record.columns().entrySet()) {
            if (e.getValue().isChanged()) {
                symbols.add(e.getKey());
            }
        }
        return symbols;
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
    protected abstract ExceptionType getExceptionType(SQLException sqlException);

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


    @Deprecated
    public Query buildSelectQuery(Table table, Composite.Value value) {
        Query query = new Query(this);
        query.append("SELECT * FROM #1#", table);
        if (value != null) {
            query.append(" WHERE #1#", value);
        }
        return query;
    }

    @Deprecated
    public Query buildSelectQuery(Table table, Composite key, Collection<?> values) {
        Query query = new Query(this);
        if (key.isSingle()) {
            query.append("SELECT * FROM #1# WHERE #2# IN (#3#)", table, key, values);
        } else if (this instanceof Postgres) {
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

    @Deprecated
    public Query toSqlExpression(Value value) {
        Query query = new Query(this);
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

    @Deprecated
    public Query buildDeleteQuery(Table table, Composite.Value value) {
        Query query = new Query(this);
        query.append("DELETE FROM #1#", table);
        if (value != null) {
            query.append(" WHERE #1#", value);
        }
        return query;
    }

}
