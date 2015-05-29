package com.jajja.jorm.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import com.jajja.jorm.Query;
import com.jajja.jorm.Record;
import com.jajja.jorm.Record.ResultMode;
import com.jajja.jorm.Symbol;
import com.jajja.jorm.Row.Column;

public class Oracle extends Standard {

    public static final Appender INSERT_ALL = new InsertAll();

    private static final Appender[] INSERT_APPENDERS =  new Appender[] {
        Oracle.INSERT_ALL
    };

    protected Oracle(String database, Connection connection) throws SQLException {
        super(database, connection);
    }

    @Override
    public int getMaxParameters() {
        return Integer.MAX_VALUE;
    }

    @Override
    public String getCurrentDateExpression() {
        return "curdate()";
    }

    @Override
    public String getCurrentTimeExpression() {
        return "curtime()";
    }

    @Override
    public String getCurrentDatetimeExpression() {
        return "now()";
    }

    @Override
    public Appender[] getInsertAppenders() {
        return INSERT_APPENDERS;
    }

    private static class InsertAll extends Appender {
        @Override
        public void append(Data data, Query query, ResultMode mode) {
            query.append("INSERT ALL");
            for (Record record : data.records) {
                Set<Symbol> symbols = data.changedSymbols.isEmpty() ? data.pkSymbols : data.changedSymbols;
                query.append(" INTO #1# (", data.table);
                boolean comma = false;
                for (Symbol symbol : data.changedSymbols) {
                    if (comma) {
                        query.append(", ");
                    } else {
                        comma = true;
                    }
                    query.append("#:1#", symbol);
                }
                query.append(") VALUES (");
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
            }
            query.append(" SELECT * FROM dual");
        }
    }

}
