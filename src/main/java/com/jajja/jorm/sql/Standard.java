package com.jajja.jorm.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import com.jajja.jorm.Query;
import com.jajja.jorm.Record;
import com.jajja.jorm.Record.ResultMode;
import com.jajja.jorm.Symbol;
import com.jajja.jorm.Row.Column;

public class Standard extends Sql {

    public static final Appender INSERT_INTO = new InsertInto();
    public static final Appender VALUES = new Values();
    public static final Appender DELETE_FROM = new DeleteFrom();
    public static final Appender WHERE = new Where();

    private static final Appender[] INSERT_APPENDERS =  new Appender[] {
        INSERT_INTO,
        VALUES
    };

    private static final Appender[] UPDATE_APPENDERS =  new Appender[] {
    };

    private static final Appender[] DELETE_APPENDERS =  new Appender[] {
        DELETE_FROM,
        WHERE
    };

    protected Standard(String database, Connection connection) throws SQLException {
        super(database, connection);
    }

    @Override
    public int getMaxParameters() {
        return Integer.MAX_VALUE;
    }

    @Override
    public String getCurrentDateExpression() {
        return "CURRENT_DATE";
    }

    @Override
    public String getCurrentTimeExpression() {
        return "CURRENT_TIME";
    }

    @Override
    public String getCurrentDatetimeExpression() {
        return "CURRENT_TIMESTAMP";
    }

    @Override
    public Appender[] getAppenders(Operation operation) {
        switch(operation) {
        case INSERT:
            return getInsertAppenders();
        case UPDATE:
            return getUpdateAppenders();
        case DELETE:
            return getDeleteAppenders();
        default:
            throw new IllegalStateException(String.format("The batch operation %s is unknown!", operation));
        }
    }

    protected Appender[] getInsertAppenders() {
        return INSERT_APPENDERS;
    }

    protected Appender[] getUpdateAppenders() {
        return UPDATE_APPENDERS;
    }

    protected Appender[] getDeleteAppenders() {
        return DELETE_APPENDERS;
    }

    private static class InsertInto extends Appender {
        @Override
        public void append(Data data, Query query, ResultMode mode) {
            query.append("INSERT INTO #1# (", data.table);
            boolean comma = false;
            for (Symbol symbol : data.changedSymbols) {
                if (comma) {
                    query.append(", ");
                } else {
                    comma = true;
                }
                query.append("#:1#", symbol);
            }
            query.append(")");
        }
    }

    private static class Values extends Appender {
        @Override
        public void append(Data data, Query query, ResultMode mode) {
            query.append(" VALUES ");
            boolean comma = false;
            for (Record record : data.records) {
                if (comma) {
                    query.append(", ");
                }
                query.append("(");
                Set<Symbol> symbols = data.changedSymbols.isEmpty() ? data.pkSymbols : data.changedSymbols;
                comma = false;
                for (Symbol symbol : symbols) {
                    if (comma) {
                        query.append(", ");
                    } else {
                        comma = true;
                    }
                    Column column = record.columns().get(symbol);
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
                query.append(")");
                comma = true;
            }
        }
    }

    private static class DeleteFrom extends Appender {
        @Override
        public void append(Data data, Query query, ResultMode mode) {
            query.append("DELETE FROM #1# ", data.table);
        }
    }

    private static class Where extends Appender {
        @Override
        public void append(Data data, Query query, ResultMode mode) {
            query.append("WHERE ");
            boolean or = false;
            for (Record record : data.records) {
                if (or) {
                    query.append(" OR ");
                } else {
                    or = true;
                }
                query.append("(");
                boolean and = false;
                for (Symbol symbol : data.pkSymbols) {
                    if (and) {
                        query.append(" AND ");
                    } else {
                        and = true;
                    }
                    Column column = record.columns().get(symbol);
                    if (column == null || column.getValue() == null) {
                        throw new IllegalStateException(String.format("Primary key (part) not set! (%s)", symbol));
                    } else {
                        query.append("#:1# = #?1#", symbol, column.getValue());
                    }
                }
                query.append(")");
            }
        }
    }

    @Override
    public ExceptionType getExceptionType(SQLException sqlException) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getNowFunction() {
        // TODO Auto-generated method stub
        return "now()";
    }

    @Override
    public String getNowQuery() {
        // TODO Auto-generated method stub
        return null;
    }

}
