package com.jajja.jorm.sql;

import java.sql.SQLException;
import java.util.HashMap;

import org.postgresql.util.PGobject;

import com.jajja.jorm.Query;
import com.jajja.jorm.Record;
import com.jajja.jorm.Symbol;
import com.jajja.jorm.Record.ResultMode;
import com.jajja.jorm.Row.Column;

public class Postgres extends Standard {

    private static boolean PG = false;
    static {
        try {
            Class.forName("org.postgresql.util.PGobject");
            PG = true;
        } catch (ClassNotFoundException e) {
        }
    }

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
    public static final Appender UPDATE_WHERE = new UpdateWhere();

    private static final Appender[] SELECT_APPENDERS =  new Appender[] {
        Standard.SELECT,
        Postgres.WHERE,
    };

    private static final Appender[] INSERT_APPENDERS =  new Appender[] {
        Standard.INSERT,
        Standard.VALUES,
        Postgres.RETURNING,
    };

    private static final Appender[] UPDATE_APPENDERS =  new Appender[] {
        Postgres.UPDATE_WHERE,
        Postgres.RETURNING,
    };

    private static final Appender[] DELETE_APPENDERS =  new Appender[] {
        Standard.DELETE,
        Postgres.WHERE,
        Postgres.RETURNING,
    };

    private final boolean is82;

    protected Postgres(Product product) {
        super(product);
        int major = product.getMajor();
        int minor = product.getMinor();
        is82 = major > 8 || (major == 8 && minor > 1); // returning clause & select from values
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
    public boolean isReturningSupported(Operation operation) {
        return is82;
    }

    @Override
    public boolean isBatchSupported(Operation operation) {
        return is82;
    }

    @Override
    public ExceptionType getExceptionType(SQLException sqlException) {
        ExceptionType type = EXCEPTIONS.get(sqlException.getSQLState());
        return type != null ? type : ExceptionType.UNKNOWN;
    }

    @Override
    public Appender[] getSelectAppenders() {
        if (is82) {
            return SELECT_APPENDERS;
        } else {
            return super.getInsertAppenders();
        }
    }

    @Override
    public Appender[] getInsertAppenders() {
        if (is82) {
            return INSERT_APPENDERS;
        } else {
            return super.getInsertAppenders();
        }
    }

    @Override
    public Appender[] getUpdateAppenders() {
        if (is82) {
            return UPDATE_APPENDERS;
        } else {
            return super.getInsertAppenders();
        }
    }

    @Override
    public Appender[] getDeleteAppenders() {
        if (is82) {
            return DELETE_APPENDERS;
        } else {
            return super.getDeleteAppenders();
        }
    }

    private static String getTypeName(Object object) {
        if (object instanceof java.sql.Timestamp) {
            return "timestamp";
        }
        if (object instanceof java.util.Date) {
            return "date";
        }
        if (PG && object instanceof PGobject) {
            return ((PGobject)object).getType();
        }
        return null;
    }

    public static class UpdateWhere extends Appender {
        @Override
        public void append(Data data, Query query, ResultMode mode) {
            if (data.isScalar()) {
                appendScalar(data, query, mode);
            } else {
                appendVector(data, query, mode);
            }
        }

        private void appendVector(Data data, Query query, ResultMode mode) {
            String virtual = data.table.getTable().equals("v") ? "v2" : "v";

            query.append("UPDATE #1# SET ", data.table);
            boolean comma = false;
            for (Symbol column : data.changedSymbols) {
                query.append(comma ? ", #1# = #!2#.#1#" : "#1# = #!2#.#1#", column, virtual);
                comma = true;
            }

            query.append(" FROM (VALUES ");
            comma = false;
            for (Record record : data.records) {
//              if (record.isCompositeKeyNull(primaryKey)) { // XXX: validate elsewhere!
//                  throw new IllegalArgumentException("Record has unset or NULL primary key: " + record);
//              }
                query.append(comma ? ", (" : "(");
                comma = false;
                for (Symbol column : data.changedSymbols) {
                    Object value = record.get(column);
                    if (value instanceof Query) {
                        query.append(comma ? "#1#" : ", #1#", value);
                    } else {
                        String pgDataType = getTypeName(value);
                        if (pgDataType != null) {
                            query.append(comma ? "cast(#?1# AS #:2#)" : ", cast(#?1# AS #:2#)", value, pgDataType);
                        } else {
                            query.append(comma ? "#?1#" : ", #?1#", value);
                        }
                    }
                    comma = true;
                }
                query.append(")");
                comma = true;
            }

            query.append(") #!1# (", virtual);
            comma = false;
            for (Symbol column : data.changedSymbols) {
                query.append(comma ? ", #1#" : "#1#", column);
                comma = true;
            }
            query.append(") WHERE");

            boolean and = false;
            for (Symbol symbol : data.pkSymbols) {
                query.append(and ? " AND #1#.#2# = #:3#.#2#" : " #1#.#2# = #:3#.#2#", data.table, symbol, virtual);
                and = true;
            }
        }

        private void appendScalar(Data data, Query query, ResultMode mode) {
            Record record = data.records.get(0);
            query.append("UPDATE #1# SET ", data.table);
            boolean comma = false;
            for (Symbol symbol : data.changedSymbols) {
                if (comma) {
                    query.append(", ");
                } else {
                    comma = true;
                }
                Column column = record.columns().get(symbol);
                if (!data.pkSymbols.contains(symbol)) {
                    if (column.getValue() == null) {
                        query.append("#:1# = NULL");
                    } else {
                        query.append("#:1# = #?1#", symbol, column.getValue());
                    }
                }
            }
            boolean and = false;
            for (Symbol symbol : data.pkSymbols) {
                if (and) {
                    query.append(" AND ");
                } else {
                    and = true;
                }
                Column column = record.columns().get(symbol);
//                if (column == null || column.getValue() == null) { // XXX: validate elsewhere!
//                    throw new IllegalStateException(String.format("Primary key (part) not set! (%s)", symbol));
//                } else {
                    query.append("#:1# = #?1#", symbol, column.getValue());
//                }
            }
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
