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

    private static final Appender[] INSERT_APPENDERS =  new Appender[] {
        INSERT_INTO,
        VALUES
    };

    private static final Appender[] UPDATE_APPENDERS =  new Appender[] {
    };

    private static final Appender[] DELETE_APPENDERS =  new Appender[] {
    };

    protected Standard(String database, Connection connection) throws SQLException {
        super(database, connection);
    }

    @Override
    public int getMaxParameters() {
        return Integer.MAX_VALUE;
    }

    @Override
    public Appender[] getAppenders(Operation operation) {
        switch(operation) {
        case INSERT:
            return INSERT_APPENDERS;
        case UPDATE:
            return UPDATE_APPENDERS;
        case DELETE:
            return DELETE_APPENDERS;
        default:
            throw new IllegalStateException(String.format("The batch operation %s is unknown!", operation));
        }
    }

    private static class InsertInto extends Appender {
        @Override
        public void append(Data data, Query query, ResultMode mode) {
            query.append("INSERT INTO #1# (", data.table);
            boolean isFirst = true;
            for (Symbol symbol : data.changedSymbols) {
                query.append(isFirst ? "#:1#" : ", #:1#", symbol);
                isFirst = false;
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



    // XXX remove?

    @Override
    public ReturnSetSyntax getReturnSetSyntax() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ExceptionType getExceptionType(SQLException sqlException) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getNowFunction() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getNowQuery() {
        // TODO Auto-generated method stub
        return null;
    }

}