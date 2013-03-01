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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
 *         return get(&quot;locale_id&quot;, Locale.class); // XXX: caches referenced record
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
    private String databaseName;
    Map<Symbol, Field> fields = new HashMap<Symbol, Field>();
    private Table table;
    private boolean isStale = false;
    private boolean isReadOnly = false;
    private static Map<Class<? extends Record>, Log> logs = new ConcurrentHashMap<Class<? extends Record>, Log>();

    private static class Field {
        private Object value = null;
        // XXX NEVER SET THIS TO TRUE (taint(), insert(), update(), etc) FOR ID-COLUMNS UNLESS SET EXPLICITLY AND EXTERNALLY BY set() (i.e. user forced an ID change)
        private boolean isChanged = false;
        private Record reference = null;
        
        private Field() {}

        public void setValue(Object value) {
            this.value = value;
        }

        public Object getValue() {
            return value;
        }

        public void setChanged(boolean isChanged) {
            this.isChanged = isChanged;
        }

        public boolean isChanged() {
            return isChanged;
        }

        public void setReference(Record reference) {
            this.reference = reference;
        }
        
        public Record getReference() {
            return reference;
        }
    }

    private Field getOrCreateField(Symbol symbol) {
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
    public static Log log(Class<? extends Record> clazz) {
        Log log = logs.get(clazz);
        if (log == null) {
            log = LogFactory.getLog(clazz);
            logs.put(clazz, log);
        }
        return log;
    }
    
    /**
     * Provides the cached log for the instance class according to {@link Record#log(Class)}.
     */
    public Log log() {
        return log(getClass());
    }
    
    
    /**
     * Constructs a mapped record. Depends on {@link Jorm} annotation for table
     * mapping.
     */
    public Record() {
        table = Table.get(getClass());
        databaseName = table.getDatabase();
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
        databaseName = table.getDatabase();
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

    /**
     * Binds a record class to a thread local transaction. Requires the given
     * class to be mapped by {@link Jorm}.
     * 
     * @param clazz
     *            the mapped record class.
     * @return the current thread local transaction for the given class.
     */
    public static Transaction bind(Class<? extends Record> clazz) {
        return Database.open(Table.get(clazz).getDatabase());
    }

    public Object id() {
        return get(table.getId());
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
     * Provides the transaction used by this record.
     * 
     * @return the transaction.
     */
    public Transaction transaction() {
        return Database.open(databaseName);
    }

    /**
     * Provides the SQL dialect used by the transaction of the record.
     * 
     * @return the SQL dialect.
     */
    public Dialect dialect() {
        return transaction().getDialect();
    }

    /**
     * Populates the record with the first result for which the given column name
     * matches the given value.
     * 
     * @param column
     *            the column name.
     * @param value
     *            the value to match.
     * @return true if the record could be updated with a matching row from the table.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public boolean populateByColumn(String column, Object value) throws SQLException { // XXX: javadoc from here
        return populateByColumn(Symbol.get(column), value);
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
    public boolean populateByColumn(Symbol symbol, Object value) throws SQLException {
        return selectInto("SELECT * FROM #1# WHERE #:2# = #3#", table, symbol, value); // XXX: fork!
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
        return populateByColumn(table.getId(), id);
    }

    private static <T extends Record> Query getSelectQuery(Class<T> clazz, Column... columns) {
        Dialect dialect = bind(clazz).getDialect();
        Query query = Table.get(clazz).getSelectQuery(dialect);
        boolean isFirst = true;
        for (Column column : columns) {
            if (isFirst) {
                isFirst = false;
                query.append(" WHERE #:1# = #2# ", column.getSymbol(), column.getValue());
            } else {
                query.append(" AND #:1# = #2# ", column.getSymbol(), column.getValue());
            }
        }
        return query;
    }
    
    /**
     * Builds a generic SQL query for the record.
     * 
     * @param sql
     *            the SQL statement to represent the query.
     * @return the built query.
     */
    public Query build(String sql) {
        return new Query(dialect(), sql);
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
        return new Query(dialect(), sql, params);
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
        return new Query(bind(clazz).getDialect(), sql);
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
        return new Query(bind(clazz).getDialect(), sql, params);
    }
    
    /**
     * Provides a selected record from the mapped database table, populated with
     * the first result for which the columns match.
     * 
     * @param clazz
     *            the class defining the table mapping.
     * @param columns
     *            the column(s) defining the record.
     * @return the matched record or null for no match.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static <T extends Record> T find(Class<T> clazz, Column... columns) throws SQLException {
        return select(clazz, getSelectQuery(clazz, columns));
    }
    
    /**
     * Provides a complete list of selected records from the mapped database
     * table, populated with the results for which the columns match.
     * 
     * @param clazz
     *            the class defining the table mapping.
     * @param columns
     *            the column(s) defining this record.
     * @return the matched records.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static <T extends Record> List<T> findAll(Class<T> clazz, Column... columns) throws SQLException {
        return selectAll(clazz, getSelectQuery(clazz, columns));
    }

    /**
     * Provides a complete list of selected reference records of a given class
     * referring to the mapped record through a given foreign key column.
     * 
     * @param clazz
     *            the class of the records referring to the mapped record.
     * @param column
     *            the column defining the foreign key for the reference records.
     * @return the matched references.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public <T extends Record> List<T> findReferences(Class<T> clazz, String column) throws SQLException {
        return findReferences(clazz, Symbol.get(column));
    }
    
    /**
     * Provides a complete list of selected reference records of a given class
     * referring to the mapped record through a given foreign key column.
     * 
     * @param clazz
     *            the class of the records referring to the mapped record.
     * @param symbol
     *            the symbol of the column defining the foreign key for the
     *            reference records.
     * @return the matched references.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public <T extends Record> List<T> findReferences(Class<T> clazz, Symbol symbol) throws SQLException {
        Table table = Table.get(clazz);
        return selectAll(clazz, "SELECT * FROM #1# WHERE #:2# = #3#", table, symbol, get(symbol));
    }

    
    /**
     * Provides a selected record, populated with the result for which the id
     * column matches the given id value.
     * 
     * @param clazz
     *            the class defining the table mapping.
     * @param id
     *            the match value.
     * @return the matched record or null for no match.
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static <T extends Record> T findById(Class<T> clazz, Object id) throws SQLException {
        return find(clazz, new Column(Table.get(clazz).getId(), id));
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
        T record = construct(clazz);
        if (record.selectInto(sql, params)) {
            return record;
        }
        return null;
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
        T record = construct(clazz);
        if (record.selectInto(query)) {
            return record;
        }
        return null;
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
        return selectAll(clazz, new Query(bind(clazz).getDialect(), sql, params));
    }

    /**
     * Provides a list of selected records, populated with the results from the
     * given query.
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
        PreparedStatement preparedStatement = bind(clazz).prepare(query.getSql(), query.getParams());
        ResultSet resultSet = null;
        LinkedList<T> records = new LinkedList<T>();
        try {
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                T record = construct(clazz);
                record.populate(resultSet);
                records.add(record);
            }
        } catch (SQLException sqlException) {
            bind(clazz).getDialect().rethrow(sqlException, query.getSql());
        } finally {
            if (resultSet != null) resultSet.close();
            preparedStatement.close();
        }
        return records;   
    }

//    /**
//     * Provides a hash map of selected records, populated with the results from the
//     * given query.
//     * 
//     * @param clazz
//     *            the class defining the table mapping.
//     * @param column
//     *            the column to use as key.
//     * @param query
//     *            the query.
//     * @return the matched records.
//     * @throws SQLException
//     *             if a database access error occurs or the generated SQL
//     *             statement does not return a result set.
//     */
//    public static <T extends Record> Map<Object, T> selectAllAsMap(Class<T> clazz, Symbol column, Query query) throws SQLException {
//        PreparedStatement preparedStatement = bind(clazz).prepare(query.getSql(), query.getParams());
//        ResultSet resultSet = null;
//        HashMap<Object, T> records = new HashMap<Object, T>();
//        try {
//            resultSet = preparedStatement.executeQuery();
//            while (resultSet.next()) {
//                T record = construct(clazz);
//                record.populate(resultSet);
//                records.put(record.get(column), record);
//            }
//        } catch (SQLException sqlException) {
//            bind(clazz).getDialect().rethrow(sqlException, query.getSql());
//        } finally {
//            if (resultSet != null) resultSet.close();
//            preparedStatement.close();
//        }
//        return records;   
//    }
//    public static <T extends Record> Map<Object, T> selectAllHashMap(Class<T> clazz, Symbol column, String sql, Object... params) throws SQLException {
//        return selectAllHashMap(clazz, column, new Query(bind(clazz).getDialect(), sql, params));
//    }
//    public static <T extends Record> Map<Object, T> selectAllHashMap(Class<T> clazz, String column, Query query) throws SQLException {
//        return selectAllHashMap(clazz, Symbol.get(column), query);
//    }
//    public static <T extends Record> Map<Object, T> selectAllHashMap(Class<T> clazz, String column, String sql, Object... params) throws SQLException {
//        return selectAllHashMap(clazz, Symbol.get(column), new Query(bind(clazz).getDialect(), sql, params));
//    }

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
        return selectInto(new Query(transaction().getDialect(), sql, params));
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
        PreparedStatement preparedStatement = transaction().prepare(query.getSql(), query.getParams());
        ResultSet resultSet = null;
        try {
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                populate(resultSet);
                return true;
            }
        } catch (SQLException sqlException) {
            transaction().getDialect().rethrow(sqlException, query.getSql());
        } finally {
            if (resultSet != null) resultSet.close();
            preparedStatement.close();
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
    public static <T extends Record> List<T> prefetch(Collection<? extends Record> records, Symbol foreignKeySymbol, Class<T> clazz, Symbol referredSymbol) throws SQLException {
        Set<Object> values = new HashSet<Object>();

        for (Record record : records) {
            Field field = record.fields.get(foreignKeySymbol);
            if (field != null && field.getValue() != null && field.getReference() == null) {
                values.add(field.getValue());
            }
        }

        if (values.isEmpty()) {
            return new LinkedList<T>();
        }

        List<T> referenceRecords = selectAll(clazz, "SELECT * FROM #1# WHERE #2# IN (#3#)", Table.get(clazz), referredSymbol, values);
        Map<Object, Record> map = new HashMap<Object, Record>();
        for (Record referenceRecord : referenceRecords) {
            map.put(referenceRecord.get(referredSymbol), referenceRecord);
        }

        for (Record record : records) {
            Field field = record.fields.get(foreignKeySymbol);
            if (field != null && field.getValue() != null && field.getReference() == null) {
                Record referenceRecord = map.get(field.getValue());
                if (referenceRecord == null) {
                    throw new IllegalStateException(field.getValue() + " not present in " + Table.get(clazz).getTable() + "." + referredSymbol.getName());
                }
                record.set(foreignKeySymbol, referenceRecord);
            }
        }

        return referenceRecords;
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
    public static <T extends Record> List<T> prefetch(Collection<? extends Record> records, String foreignKeySymbol, Class<T> clazz, String referredSymbol) throws SQLException {
        return prefetch(records, Symbol.get(foreignKeySymbol), clazz, Symbol.get(referredSymbol));
    }

    /**
     * Populates the record with the first row of the result. Any values in the
     * record object are cleared if the record was previously populated.
     * 
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public void populate(ResultSet resultSet) throws SQLException {
        isStale = false;
        try {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            HashSet<Symbol> symbols = new HashSet<Symbol>();
            
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                Symbol symbol = Symbol.get(resultSetMetaData.getColumnLabel(i));

                put(symbol, resultSet.getObject(i));
                symbols.add(symbol);
            }

            purify();

            Iterator<Symbol> i = fields.keySet().iterator();
            while (i.hasNext()) {
                Symbol symbol = i.next();
                if (!symbols.contains(symbol)) {
                    unset(symbol);
                }
            }
        } catch (SQLException sqlException) {
            transaction().getDialect().rethrow(sqlException);
        } finally {
            isStale = true; // lol exception
        }
        isStale = false;
    }

    /**
     * Save the record. This is done by a call to {@link #insert()} if the id
     * field is null, unset or changed, otherwise by a call to {@link #update()}.
     * 
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public void save() throws SQLException {
        checkReadOnly();
        Field field = fields.get(table.getId());
        if (field == null || field.getValue() == null || field.isChanged()) {
            insert();
        } else {
            update();
        }
    }
    
    /**
     * <p>
     * Commits the thread local transaction the mapped record binds to.
     * </p>
     * <p>
     * <strong>Note:</strong> This may cause changes of other records to be
     * persisted to the mapped database of the record, since all records mapped
     * to the same named database share transaction per thread.
     * </p>
     * 
     * @throws SQLException
     *             if a database access error occurs.
     */
    public void commit() throws SQLException {
        transaction().commit();
    }

    /**
     * <p>
     * Flushes the changes to the database made in the thread local transaction
     * the mapped record binds to. A shorthand for {@link #save()} followed by
     * {@link #commit()}.
     * </p>
     * <p>
     * <strong>Note:</strong> This may cause changes of other records to be
     * persisted to the mapped database of the record, since all records mapped
     * to the same named database share transaction per thread.
     * </p>
     * 
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public void saveAndCommit() throws SQLException {
        save();
        commit();
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
        checkReadOnly();
        refresh();
        Query query = new Query(transaction().getDialect(), "DELETE FROM #1# WHERE #:2# = #3#", table, table.getId(), get(table.getId()));
        PreparedStatement preparedStatement = transaction().prepare(query);
        preparedStatement.execute();
        preparedStatement.close();
        put(table.getId(), null);
    }

    /**
     * Marks all fields as changed.
     */
    public void taint() {
        for (Entry<Symbol, Field> entry : fields.entrySet()) {
            Symbol symbol = entry.getKey();
            Field field = entry.getValue();
            if (!table.isImmutable(symbol) && !symbol.equals(table.getId())) {
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

    /**
     * Marks this record as stale. It will be re-populated on the next call to
     * {@link #set(String, Object)}, {@link #set(Symbol, Object)},
     * {@link #get(String)}, {@link #get(Symbol)} or {@link #refresh()},
     * whichever comes first.
     */
    public void markStale() { // XXX: setStale(boolean) ??
        isStale = true;
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
        return isStale;
    }

    /**
     * Inserts the record's changed values into the database by executing an SQL INSERT query.
     * The record's primary key value is set to the primary key generated by the database.
     * 
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public void insert() throws SQLException {
        checkReadOnly();

        if (isStale) {
            throw new IllegalStateException("Attempting to insert stale record");
        }

        Query query = new Query(transaction().getDialect());

        query.append("INSERT INTO #1# (", table);

        boolean isFirst = true;
        for (Entry<Symbol, Field> entry : fields.entrySet()) {
            if (entry.getValue().isChanged()) {
                query.append(isFirst ? "#:1#" : ", #:1#", entry.getKey());
                isFirst = false;
            }
        }

        if (isFirst) { // No fields are marked as changed, but we need to insert something...
            query.append("#:1#) VALUES (DEFAULT)", table.getId());
        } else {
            query.append(") VALUES (");
            isFirst = true;
            for (Field field : fields.values()) {
                if (field.isChanged()) {
                    query.append(isFirst ? "#1#" : ", #1#", field.getValue());
                    isFirst = false;
                }
            }
            query.append(")");
        }

        markStale();
        if (transaction().getDialect().isReturningSupported()) {
            query.append(" RETURNING *");
            selectInto(query);
        } else {
            PreparedStatement preparedStatement = transaction().prepare(query.getSql(), query.getParams(), true);
            ResultSet resultSet = null;
            Object id = null;
            try {
                preparedStatement.execute();
                resultSet = preparedStatement.getGeneratedKeys();
                if (resultSet.next()) {
                    id = resultSet.getObject(1);
                }
            } finally {
                if (resultSet != null) resultSet.close();
                preparedStatement.close();
            }

            if (id == null) {
                throw new RuntimeException("INSERT to " + table.toString() + " did not generate a key (aka insert id): " + query.getSql());
            }
            Field field = getOrCreateField(table.getId());
            field.setValue(id);
            field.setChanged(false);
        }
    }

    /**
     * Executes a batch INSERT (INSERT INTO ... (columns...) VALUES (row1), (row2), (row3), ...) and repopulates the list with stored entities.
     * 
     * @param records List of records to insert (must be of the same class, and bound to the same DbConnection)
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static void insert(Collection<? extends Record> records) throws SQLException {
        insert(records, 0, true);
    }

    /**
     * Executes a batch INSERT (INSERT INTO ... (columns...) VALUES (row1), (row2), (row3), ...).
     *
     * For large sets of records, the use of chunkSize is recommended to avoid out-of-memory errors and too long SQL queries. 
     *
     * Setting isFullRepopulate to true will re-populate the record fields with fresh values. This will generate
     * an additional SELECT query for every chunk of records for databases that do not support RETURNING. 
     *
     * @param records List of records to insert (must be of the same class, and bound to the same DbConnection)
     * @param chunkSize Splits the records into chunks, <= 0 disables
     * @param isFullRepopulate Whether or not to fully re-populate the record fields, or just update their primary key value and markStale()
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public static void insert(Collection<? extends Record> records, int chunkSize, boolean isFullRepopulate) throws SQLException {
        Set<Symbol> columns = new HashSet<Symbol>();
        List<Record[]> chunks = new LinkedList<Record[]>();
        Record[] chunk = null;
        Record template = null;
        int size = records.size();
        int i = 0;

        if (chunkSize <= 0) {
            chunkSize = size;
        }

        for (Record record : records) {
            record.checkReadOnly();

            if (template == null) {
                template = record;
            }

            if (!template.getClass().equals(record.getClass())) {
                throw new IllegalArgumentException("all records must be of the same class");
            }
            if (!template.databaseName.equals(record.databaseName)) {
                throw new IllegalArgumentException("all records must be bound to the same DbConnection");
            }

            if (chunk == null || i >= chunkSize) {
                chunk = new Record[Math.min(size, chunkSize)];
                chunks.add(chunk);
                size -= chunk.length;
                i = 0;
            }

            columns.addAll( record.fields.keySet() );

            chunk[i++] = record;
        }

        if (template == null) {
            return;
        }

        for (Record[] recordArray : chunks) {
            batchInsert(template, columns, recordArray, isFullRepopulate);
        }
    }

    private static void batchInsert(final Record template, Set<Symbol> columns, Record[] records, final boolean isFullRepopulate) throws SQLException {
        Table table = template.table;
        Transaction transaction = template.transaction();
        Dialect dialect = transaction.getDialect();
        Query query = new Query(dialect);

        if (table.getId() != null) {
            columns.add(table.getId());
        }

        query.append("INSERT INTO #1# (", table);

        boolean isFirst = true;
        for (Symbol column : columns) {
            if (!table.isImmutable(column)) {
                query.append(isFirst ? "#:1#" : ", #:1#", column);
                isFirst = false;
            }
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
            for (Symbol column : columns) {
                if (!table.isImmutable(column)) {
                    if (record.isFieldChanged(column)) {
                        query.append(isColumnFirst ? "#1#" : ", #1#", record.get(column));
                    } else {
                        query.append(isColumnFirst ? "DEFAULT" : ", DEFAULT");
                    }
                    isColumnFirst = false;
                }
            }
            query.append(")");
            record.markStale();
        }

        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            boolean usingReturning = isFullRepopulate && dialect.isReturningSupported();
            Map<Object, Record> map = null;

            if (usingReturning) {
                query.append(" RETURNING *");
                preparedStatement = transaction.prepare(query.getSql(), query.getParams());
                resultSet = preparedStatement.executeQuery();
            } else {
                preparedStatement = transaction.prepare(query.getSql(), query.getParams(), true);
                preparedStatement.execute();
                resultSet = preparedStatement.getGeneratedKeys();
                if (isFullRepopulate) {
                    map = new HashMap<Object, Record>();
                }
            }

            for (Record record : records) {
                if (!resultSet.next()) {
                    throw new IllegalStateException("too few rows returned?");
                }
                if (usingReturning) {
                    // RETURNING rocks!
                    record.populate(resultSet);
                } else {
                    Field field = record.getOrCreateField(table.getId());
                    field.setValue(resultSet.getObject(1));
                    field.setChanged(false);
                    if (isFullRepopulate) {
                        map.put(field.getValue(), record);
                        record.isStale = false; // actually still stale
                    }
                }
            }

            if (!usingReturning && isFullRepopulate) {
                resultSet.close();
                resultSet = null;
                preparedStatement.close();
                preparedStatement = null;

                // records must not be stale, or Query will generate SELECTs
                Query q = table.getSelectQuery(dialect).append("WHERE #1# IN (#2:id#)", table.getId(), records);

                preparedStatement = transaction.prepare(q);
                resultSet = preparedStatement.executeQuery();

                int idColumn = resultSet.findColumn(table.getId().getName());
                if (Dialect.DatabaseProduct.MYSQL.equals(dialect.getDatabaseProduct())) {
                    while (resultSet.next()) {
                        map.get(resultSet.getLong(idColumn)).populate(resultSet);
                    }
                } else {
                    while (resultSet.next()) {
                        map.get(resultSet.getObject(idColumn)).populate(resultSet);
                    }
                }
            }
        } catch (SQLException sqlException) {
            // records are in an unknown state, mark them stale
            for (Record record : records) {
                record.markStale();
            }
            transaction.getDialect().rethrow(sqlException);
        } finally {
            if (resultSet != null) resultSet.close();
            if (preparedStatement != null) preparedStatement.close();
        }
    }

    /**
     * Updates the record's changed column values by executing an SQL UPDATE query.
     * 
     * @throws SQLException
     *             if a database access error occurs or the generated SQL
     *             statement does not return a result set.
     */
    public void update() throws SQLException {
        checkReadOnly();

        if (!isChanged()) {
            return;
        }
        if (isStale) {
            // unreachable?
            throw new IllegalStateException("Attempting to update a stale record!");
        }

        Query query = new Query(transaction().getDialect());

        query.append("UPDATE #1# SET ", table);

        boolean isFirst = true;
        for (Entry<Symbol, Field> entry : fields.entrySet()) {
            if (entry.getValue().isChanged()) {
                if (isFirst) {
                    isFirst = false;
                } else {
                    query.append(", ");
                }
                query.append("#:1# = #2#", entry.getKey(), entry.getValue().getValue());
            }
        }

        Object id = get(table.getId());

        if (id == null) throw new IllegalStateException("Attempting to update record without id!");

        query.append(" WHERE #:1# = #2#", table.getId(), id);

        markStale();
        if (transaction().getDialect().isReturningSupported()) {
            query.append(" RETURNING *");
            selectInto(query);
        } else {
            transaction().executeUpdate(query);
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
        if (isStale) {
            try {
                Field field = fields.get(table.getId());
                if (field == null || field.getValue() == null) {
                    throw new NullPointerException("Attempted to refresh record with non-existent or null id");
                }
                populateById(field.getValue());
            } catch (SQLException e) {
                throw new RuntimeException("Failed to refresh stale record", e);
            }
            isStale = false;
        }
    }

    /**
     * Sets the record as read only according to given value
     * 
     * @param isReadOnly
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
    public void readOnly(boolean isReadOnly) {
        if (table.getId() == null && isReadOnly) {
            throw new RuntimeException("Cannot mark anonymous records as read only!");
        }
        this.isReadOnly = isReadOnly;
    }

    /**
     * Returns true if this record is read only.
     */
    public boolean isReadOnly() {
        return isReadOnly;
    }

    private void checkReadOnly() {
        if (isReadOnly) {
            throw new RuntimeException("Record is read only!");
        }
    }

    private boolean isChanged(Symbol symbol, Object newValue) {
        if (isReadOnly || table.isImmutable(symbol)) {
            return false;
        }
        Field field = fields.get(symbol);
        if (field != null) {
            Object oldValue = field.getValue();
            return (oldValue == null ^ newValue == null) || oldValue == null || !oldValue.equals(newValue);
        }
        return true;
    }

    private void put(Symbol symbol, Object value) {
        refresh();

        boolean isChanged;
        Field field = fields.get(symbol);
        if (field == null) {
            field = new Field();
        }

        if (value != null && isRecordSubclass(value.getClass())) {
            Record record = (Record)value;
            Object id = record.get(record.table.getId(), Object.class);
            if (id == null) {
                throw new NullPointerException("While setting " + record + "." + symbol.getName() + " = " + value + " -- id (primary key) is null -- perhaps you need to save()?");
            }
            if (isChanged = isChanged(symbol, id)) {
                notifyFieldChanged(symbol, value);                
            }
            field.setReference(record);
            field.setValue(id);
        } else {
            if (isChanged = isChanged(symbol, value)) {
                notifyFieldChanged(symbol, value);                
            }
            if (isChanged) {
                field.setReference(null); // invalidate cached reference
            }
            field.setValue(value);
        }

        if (isChanged) {
            // it's OK to mark the id column has changed here
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
        checkReadOnly();
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
        checkReadOnly();
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
        return getField(Symbol.get(column), clazz, false);
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
        return getField(symbol, clazz, false);
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
	public <T extends Record> T get(Symbol symbol, Class<T> clazz, boolean isCacheOnly)  {
	    return getField(symbol, clazz, isCacheOnly);
	}
    
    @SuppressWarnings("unchecked")
	private <T> T getField(Symbol symbol, Class<T> clazz, boolean isCacheOnly) {
        refresh();

    	Field field = fields.get(symbol);
    	if (field == null) {
    	    return null;
    	}

    	Object value = field.getValue();

    	if (value != null) {
    	    if (isRecordSubclass(clazz)) {
    	        // Load foreign key
    	        if ((field.getReference() == null) && !isCacheOnly) {
    	            try {
    	                Record reference = Record.findById((Class<? extends Record>)clazz, value);
    	                field.setReference(reference);
    	                value = reference;
    	            } catch (SQLException e) {
    	                throw new RuntimeException("failed to findById(" + clazz + ", " + value + ")", e);
    	            }
    	        } else {
    	            value = field.getReference();
    	        }
    	    } else if (!clazz.isAssignableFrom(value.getClass())) {
    			throw new RuntimeException("column " + symbol.getName() + " is of type " + value.getClass() + ", but " + clazz + " was requested");
    		}
    	}
        return (T) value;
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

    	refresh();

    	if (table.getSchema() != null) {
    	    stringBuilder.append(table.getSchema());
    	    stringBuilder.append('.');
    	}
        if (table.getTable() != null) {
            stringBuilder.append(table.getTable());
        }
    	stringBuilder.append(" { ");

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
            return ((Record) object).get(table.getId()).equals(get(table.getId()));            
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return get(table.getId()).hashCode();
    }
}
