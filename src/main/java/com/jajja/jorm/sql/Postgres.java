package com.jajja.jorm.sql;

import java.sql.Connection;
import java.sql.SQLException;
import com.jajja.jorm.Query;
import com.jajja.jorm.Record;
import com.jajja.jorm.Symbol;
import com.jajja.jorm.Record.ResultMode;
import com.jajja.jorm.Row.Column;

public class Postgres extends Standard {

    public static final Appender RETURNING = new Returning();
    public static final Appender WHERE = new Where();

    private static final Appender[] INSERT_APPENDERS =  new Appender[] {
        Standard.INSERT_INTO,
        Standard.VALUES,
        Postgres.RETURNING
    };

    private static final Appender[] DELETE_APPENDERS =  new Appender[] {
        Standard.DELETE_FROM,
        Postgres.WHERE,
        Postgres.RETURNING
    };

    protected Postgres(String database, Connection connection) throws SQLException {
        super(database, connection);
    }

    @Override
    public int getMaxParameters() {
        return 32768;
    }

    @Override
    public Appender[] getInsertAppenders() {
        return INSERT_APPENDERS;
    }

    @Override
    public Appender[] getDeleteAppenders() {
        return DELETE_APPENDERS;
    }

    public static class Returning extends Appender {
        @Override
        public void append(Data data, Query query, ResultMode mode) {
            switch (mode) {
            case ID_ONLY:
                query.append(" RETURNING");
                boolean comma = false;
                for (Symbol symbol : data.table.getPrimaryKey().getSymbols()) {
                    if (comma) {
                        query.append(", #1#", symbol);
                    } else {
                        query.append(" #1#", symbol);
                        comma = true;
                    }
                }
                break;

            case REPOPULATE:
                query.append(" RETURNING *");
                break;

            case NO_RESULT:
                break;
            }
        }
    }

    private static class Where extends Appender {
        @Override
        public void append(Data data, Query query, ResultMode mode) {
            query.append("WHERE ");
            boolean comma = false;
            query.append("(");
            for (Symbol symbol : data.pkSymbols) {
                if (comma) {
                    query.append(", ");
                } else {
                    comma = true;
                }
                query.append("#:1#", symbol);
            }
            query.append(") IN (");
            comma = false;
            for (Record record : data.records) {
                if (comma) {
                    query.append(", ");
                }
                query.append("(");
                comma = false;
                for (Symbol symbol : data.pkSymbols) {
                    if (comma) {
                        query.append(", ");
                    } else {
                        comma = true;
                    }
                    Column column = record.columns().get(symbol);
                    if (column == null || column.getValue() == null) {
                        throw new IllegalStateException(String.format("Primary key (part) not set! (%s)", symbol));
                    } else {
                        query.append("#?1#", symbol, column.getValue());
                    }
                }
                query.append(")");
                comma = true;
            }
        }
    }

}
