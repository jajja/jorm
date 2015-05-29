package com.jajja.jorm.sql;

import java.sql.SQLException;
import java.util.HashMap;

import com.jajja.jorm.Query;
import com.jajja.jorm.Record;
import com.jajja.jorm.Symbol;
import com.jajja.jorm.Record.ResultMode;
import com.jajja.jorm.Row.Column;

public class Postgres extends Standard {

    private static final HashMap<String, ExceptionType> EXCEPTIONS = new HashMap<String, ExceptionType>();
    static {
        EXCEPTIONS.put("23503", ExceptionType.FOREIGN_KEY_VIOLATION);     // foreign_key_violation
        EXCEPTIONS.put("23505", ExceptionType.UNIQUE_VIOLATION);          // unique_violation
        EXCEPTIONS.put("23514", ExceptionType.CHECK_VIOLATION);           // check_violation
        EXCEPTIONS.put("40P01", ExceptionType.DEADLOCK_DETECTED);         // deadlock_detected
        EXCEPTIONS.put("55P03", ExceptionType.LOCK_TIMEOUT);              // lock_not_available
    }

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

    private final boolean isReturning;

    protected Postgres(Product product) {
        super(product);
        int major = product.getMajor();
        int minor = product.getMinor();
        isReturning = major > 8 || (major == 8 && minor > 1);
    }

    @Override
    public int getMaxParameters() {
        return 32768;
    }

    @Override
    public String getCurrentDatetimeExpression() {
        return "now()";
    }

    @Override
    public boolean isReturningSupported() {
        return isReturning;
    }

    @Override
    public ExceptionType getExceptionType(SQLException sqlException) {
        ExceptionType type = EXCEPTIONS.get(sqlException.getSQLState());
        return type != null ? type : ExceptionType.UNKNOWN;
    }

    @Override
    public Appender[] getInsertAppenders() {
        if (isReturning) {
            return INSERT_APPENDERS;
        } else {
            return super.getInsertAppenders();
        }
    }

    @Override
    public Appender[] getDeleteAppenders() {
        if (isReturning) {
            return DELETE_APPENDERS;
        } else {
            return super.getInsertAppenders();
        }
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
