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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * The implementation for generating SQL statements and mapping parameters to statements for records and transactions.
 *
 * <h3>Grammar</h3>
 * <p>
 * Processes and appends the specified sql string.<br>
 * <br>
 * Format placeholder syntax: #(modifier)[argno](:[label])#<br>
 * <br>
 * Modifiers: (none) value is quoted, (!) value is NOT quoted, (:) value is quoted as an SQL identifier<br>
 * <br>
 * When a label is specified, the referenced argument must either a Record (the label refers to a column name),
 * or a java.util.Map (the label refers to a key contained by the Map).<br>
 * <br>
 * If the referenced argument is an iterable, each entry in the iterable is processed separately,
 * then finally concatenated together using a colon as a separator.<br>
 * <br>
 * If the referenced argument is a Table, it is properly quoted (e.g "the_schema"."the_table"), regardless of modifier.<br>
 * <br>
 * If the referenced argument is a QueryBuilder, it is processed as if appended by append(). The modifier is ignored.<br>
 * <br>
 * </p>
 *
 * @see Record
 * @see Transaction
 * @author Andreas Allerdahl &lt;andreas.allerdahl@jajja.com&gt;
 * @author Martin Korinth &lt;martin.korinth@jajja.com&gt;
 * @since 1.0.0
 */
public class Query {
    public static final char MODIFIER_NONE = 0;
    public static final char MODIFIER_UNQUOTED = '!';
    public static final char MODIFIER_IDENTIFIER = ':';
    public static final char MODIFIER_RAW = '?';
    private static final String ALL_MODIFIERS = "!:?";
    private int numParams;
    private RawSql currentRawSql;
    final LinkedList<Object> parts = new LinkedList<Object>();

    private static class RawSql {
        private StringBuilder sql = new StringBuilder();

        @Override
        public String toString() {
            return sql.toString();
        }
    }

    private static class Parameter {
        private final Object parameter;

        public Parameter(Object param) {
            this.parameter = param;
        }

        @Override
        public String toString() {
            if (parameter != null) {
                return "'" + parameter.toString().replace("'", "''") + "'";
            }
            return "NULL";
        }
    }

    private static class Identifier {
        private final String identifier;

        public Identifier(String identifier) {
            this.identifier = identifier;
        }

        @Override
        public String toString() {
            return "\"" + identifier.toString() + "\"";
        }
    }

    public Query() {
    }

    public Query(String sql) {
        append(sql);
    }

    public Query(String sql, Object... params) {
        append(sql, params);
    }

    @Deprecated
    public Query(Dialect dialect) throws SQLException {
        this();
    }

    @Deprecated
    public Query(Dialect dialect, String sql) throws SQLException {
        this(sql);
    }

    @Deprecated
    public Query(Dialect dialect, String sql, Object ... params) throws SQLException {
        this(sql, params);
    }

    @Deprecated
    public Query(Transaction transaction) throws SQLException {
        this();
    }

    @Deprecated
    public Query(Transaction transaction, String sql) throws SQLException {
        this(sql);
    }

    @Deprecated
    public Query(Transaction transaction, String sql, Object ... params) throws SQLException {
        this(sql, params);
    }

    public Query(Query query) {
        append(query);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void append(char modifier, Object param, String label) {
        // TODO: refactor, plz
        if (param instanceof Map) {
            if (label == null) throw new IllegalArgumentException("Cannot append map without a label! (e.g. #1:map_key#)");
            param = ((Map)param).get(label);
        } else if (param instanceof Row) { // or Record
            if (label == null) throw new IllegalArgumentException("Cannot append row column without a label! (e.g. #1:foo_column#)");
            if (param instanceof Record && "@".equals(label)) {
                Record record = (Record)param;
                if (!record.table().getPrimaryKey().isSingle()) {
                    throw new UnsupportedOperationException("@ is not supported on Composite columns");
                }
                param = record.id().getValue();
            } else {
                Row row = (Row)param;
                param = row.get(label);
            }
        } else if (param instanceof Class && Record.isRecordSubclass((Class)param)) {
            param = Table.get((Class<? extends Record>)param);
        }

        if (param instanceof Table) {
            Table table = (Table)param;
            if (table.getSchema() != null) {
                appendIdentifier(table.getSchema());
                //sql.append(dialect.quoteIdentifier(table.getSchema()));
                append('.');
            }
            appendIdentifier(table.getTable());
            return;
        } else if (param instanceof Query) {
            append((Query)param);
            return;
        }

        // parse modifier
        switch (modifier) {
        case MODIFIER_UNQUOTED:
            // Raw string
            append(param != null ? param.toString() : "NULL");
            break;

        case MODIFIER_IDENTIFIER:
            // Quoted SQL identifier (table, column name, etc)
            appendIdentifier(param.toString());
            break;

        case MODIFIER_NONE:
            appendParameter(param);
            break;

        default:
            throw new IllegalStateException("Unknown modifier '" + modifier + "'");
        }
    }

    @SuppressWarnings("rawtypes")
    private void processPlaceholder(String string, Object ... params) {
        if (string.isEmpty()) {
            // UNREACHABLE
            throw new IllegalStateException("Placeholder is empty");
        }

        char modifier = string.charAt(0);
        if (ALL_MODIFIERS.indexOf(modifier) != -1) {
            string = string.substring(1);
            if (string.isEmpty()) {
                throw new IllegalStateException("Missing value after modifier");
            }
        } else {
            modifier = MODIFIER_NONE;
        }

        Object param;
        String label = null;

        int index = string.indexOf(':');
        if (index >= 0) {
            label = string.substring(index + 1);
            string = string.substring(0, index);
        }

        int i = Integer.decode(string);
        if (i - 1 >= params.length) {
            throw new IllegalStateException(String.format("Placeholder out of range: %d", i));
        }
        param = params[i-1];

        if (modifier == MODIFIER_RAW) {
            appendParameter(param);
            return;
        }

        if (param instanceof Composite) {
            param = ((Composite)param).getColumns();
            modifier = MODIFIER_IDENTIFIER;
        } else if (param instanceof Composite.Value) {
            param = ((Composite.Value)param).getValues();
        }

        if (param != null && param.getClass().isArray() && !param.getClass().getComponentType().isPrimitive()) {
            param = Arrays.asList((Object[])param);
        }
        if (param instanceof Iterable) {
            boolean isFirst = true;
            for (Object o : (Iterable)param) {
                if (isFirst) {
                    isFirst = false;
                } else {
                    append(", ");
                }
                append(modifier, o, label);
            }
        } else if (param instanceof Iterator) {
            boolean isFirst = true;
            Iterator iter = (Iterator)param;
            while (iter.hasNext()) {
                if (isFirst) {
                    isFirst = false;
                } else {
                    append(", ");
                }
                append(modifier, iter.next(), label);
            }
        } else {
            append(modifier, param, label);
        }
    }

    public Query append(String sql, Object... params) {
        int hashStart = -1;
        boolean inHash = false;

        for (int i = 0; i < sql.length(); i++) {
            char ch = sql.charAt(i);

            if (inHash) {
                if (ch == '#') {
                    processPlaceholder(sql.substring(hashStart + 1, i), params);
                    inHash = false;
                    hashStart = -1;
                }
            } else if (hashStart >= 0) {
                if (ch == '#') {
                    hashStart = -1;
                    append('#');
                    continue;
                }
                inHash = true;
            } else if (ch == '#') {
                hashStart = i;
            } else {
                append(ch);
            }
        }

        if (hashStart >= 0) {
            throw new IllegalStateException("Malformed placeholder: " + sql);
        }

        return this;
    }

    private RawSql getRawSql() {
        if (currentRawSql == null) {
            currentRawSql = new RawSql();
            parts.add(currentRawSql);
        }
        return currentRawSql;
    }

    private void appendParameter(Object parameter) {
        currentRawSql = null;
        parts.add(new Parameter(parameter));
        numParams++;
    }

    private void appendIdentifier(String identifier) {
        currentRawSql = null;
        parts.add(new Identifier(identifier));
    }

    public Query append(char sql) {
        getRawSql().sql.append(sql);
        return this;
    }

    public Query append(String sql) {
        getRawSql().sql.append(sql);
        return this;
    }

    public Query append(Query query) {
        currentRawSql = null;
        parts.addAll(query.parts);
        if (query.currentRawSql != null) {
            // Queries can't share the same currentRawSql, or they'd append to the same StringBuilder
            parts.removeLast();
            getRawSql().sql.append(query.currentRawSql.sql);
        }
        return this;
    }

    public String buildFakeSql() {
        StringBuilder sb = new StringBuilder(128);
        for (Object part : parts) {
            sb.append(part.toString());
        }
        return sb.toString();
    }

    public static class ParameterizedQuery {    // XXX rename?
        private final String sql;
        private final List<Object> parameters;

        private ParameterizedQuery(String sql, List<Object> parameters) {
            this.sql = sql;
            this.parameters = parameters;
        }

        public String getSql() {
            return sql;
        }

        public List<Object> getParameters() {
            return parameters;
        }
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
    public ParameterizedQuery build(Dialect dialect) {
        StringBuilder sql = new StringBuilder(128);
        List<Object> parameters = new ArrayList<Object>(numParams);
        for (Object part : parts) {
            if (part instanceof Query.RawSql) {
                sql.append(((Query.RawSql)part).sql);
            } else if (part instanceof Query.Identifier) {
                sql.append(dialect.quoteIdentifier(((Query.Identifier)part).identifier));
            } else if (part instanceof Query.Parameter) {
                sql.append('?');
                parameters.add(((Query.Parameter)part).parameter);
            } else {
                throw new IllegalStateException();
            }
        }
        return new ParameterizedQuery(sql.toString(), parameters);
    }

    public int getParameters() {
        return numParams;
    }

    public boolean isEmpty() {
        return parts.isEmpty();
    }
}
