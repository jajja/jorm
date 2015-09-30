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

import java.util.Arrays;
import java.util.Collection;
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
 * If the referenced argument is a Collection, each entry in the collection is processed separately,
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
    private Dialect dialect;
    private StringBuilder sql = new StringBuilder(64);
    private List<Object> params;

    Query(Dialect dialect) {
        this.dialect = dialect;
        params = new LinkedList<Object>();
    }

    Query(Dialect dialect, String sql) {
        this(dialect);
        append(sql);
    }

    Query(Dialect dialect, String sql, Object... params) {
        this(dialect);
        append(sql, params);
    }

    public Query(Transaction transaction) {
        this(transaction.getDialect());
    }

    public Query(Transaction transaction, String sql) {
        this(transaction.getDialect(), sql);
    }

    public Query(Transaction transaction, String sql, Object ... params) {
        this(transaction.getDialect(), sql, params);
    }

    public Query(Query query) {
        this(query.dialect);
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
                sql.append(dialect.quoteIdentifier(table.getSchema()));
                sql.append('.');
            }
            sql.append(dialect.quoteIdentifier(table.getTable()));
            return;
        } else if (param instanceof Symbol) {
            param = ((Symbol)param).getName();
            modifier = MODIFIER_IDENTIFIER;
        } else if (param instanceof Query) {
            append((Query)param);
            return;
        }

        // parse modifier
        switch (modifier) {
        case MODIFIER_UNQUOTED:
            // Raw string
            sql.append(param != null ? param.toString() : "NULL");
            break;

        case MODIFIER_IDENTIFIER:
            // Quoted SQL identifier (table, column name, etc)
            sql.append(dialect.quoteIdentifier(param.toString()));
            break;

        case MODIFIER_NONE:
            sql.append("?");
            this.params.add(param);
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
            sql.append("?");
            this.params.add(param);
            return;
        }

        if (param instanceof Composite) {
            param = ((Composite)param).getSymbols();
        } else if (param instanceof Composite.Value) {
            param = ((Composite.Value)param).getValues();
        }

        if (param != null && param.getClass().isArray() && !param.getClass().getComponentType().isPrimitive()) {
            param = Arrays.asList((Object[])param);
        }
        if (param instanceof Collection) {
            boolean isFirst = true;
            for (Object o : (Collection)param) {
                if (isFirst) {
                    isFirst = false;
                } else {
                    this.sql.append(", ");
                }
                append(modifier, o, label);
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
                    this.sql.append('#');
                    continue;
                }
                inHash = true;
            } else if (ch == '#') {
                hashStart = i;
            } else {
                this.sql.append(ch);
            }
        }

        if (hashStart >= 0) {
            throw new IllegalStateException("Malformed placeholder: " + sql);
        }

        return this;
    }

    public Query append(String sql) {
        this.sql.append(sql);
        return this;
    }

    public Query append(Query query) {
        sql.append(query.getSql());
        params.addAll(query.getParams());
        return this;
    }

    public String getSql() {
        return sql.toString();
    }

    public List<Object> getParams() {
        return params;
    }

    public boolean isEmpty() {
        return sql.length() == 0;
    }

    public int length() {
        return sql.length();
    }
}
