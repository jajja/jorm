package com.jajja.jorm.sql;

import java.sql.SQLException;
import java.util.Set;

import com.jajja.jorm.Query;
import com.jajja.jorm.Record;
import com.jajja.jorm.Record.ResultMode;
import com.jajja.jorm.Symbol;
import com.jajja.jorm.Row.Column;
import com.jajja.jorm.Table;

public class Standard extends Language {

    public static final Appender SELECT = new Select();
    public static final Appender INSERT = new Insert();
    public static final Appender VALUES = new Values();
    public static final Appender DELETE = new Delete();
    public static final Appender WHERE = new Where();
    public static final Appender UPDATE = new Update();

    private static final Appender[] SELECT_APPENDERS =  new Appender[] {
        SELECT,
        WHERE
    };

    private static final Appender[] INSERT_APPENDERS =  new Appender[] {
        INSERT,
        VALUES
    };

    private static final Appender[] UPDATE_APPENDERS =  new Appender[] {
        UPDATE,
        WHERE
    };

    private static final Appender[] DELETE_APPENDERS =  new Appender[] {
        DELETE,
        WHERE
    };

    public Standard(Product product) {
        super(product);
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
    public boolean isReturningSupported(Operation operation) {
        return false;
    }

    @Override
    public boolean isBatchSupported(Operation operation) {
        return false;
    }

    @Override
    public Appender[] getAppenders(Operation operation) {
        switch(operation) {
        case SELECT:
            return getSelectAppenders();
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

    protected Appender[] getSelectAppenders() {
        return SELECT_APPENDERS;
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

    private static class Select extends Appender {
        @Override
        public void append(Data data, Query query, ResultMode mode) {
            switch (mode) {
            case REPOPULATE:
                query.append("SELECT * FROM #1# ", data.table);
                break;

            case ID_ONLY: // XXX: entirely meaningless unless used outside batch context.
                query.append("SELECT ", data.table);
                boolean comma = false;
                for (Symbol symbol : data.pkSymbols) {
                    if (comma) {
                        query.append(", ");
                    } else {
                        comma = true;
                    }
                    query.append("#:1#", symbol);
                }
                query.append(" FROM #1# ", data.table);
                break;

            default:
                throw new IllegalStateException("Cannot append empty selection!");
            }
        }
    }

    private static class Insert extends Appender {
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

    private static class Delete extends Appender {
        @Override
        public void append(Data data, Query query, ResultMode mode) {
            query.append("DELETE FROM #1# ", data.table);
        }
    }

    private static class Update extends Appender {
        @Override
        public void append(Data data, Query query, ResultMode mode) {
            query.append("UPDATE #1# SET ", data.table);
            boolean comma = false;
            int i = 0;
            for (Record record : data.records) {
                if (0 < i) {
                    throw new IllegalStateException("Attempting to update av batch larger than one row, without merge support!");
                }
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
                i++;
            }
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
//                    if (column == null || column.getValue() == null) {  // XXX: validate elsewhere!
//                        throw new IllegalStateException(String.format("Primary key (part) not set! (%s)", symbol));
//                    } else {
                        query.append("#:1# = #?1#", symbol, column.getValue());
//                    }
                }
                query.append(")");
            }
        }
    }

    @Override
    public ExceptionType getExceptionType(SQLException sqlException) {
        return ExceptionType.UNKNOWN;
    }

    @Override
    public Query select(Table table) {
        Query query = new Query(this);
        SELECT.append(null, query, ResultMode.REPOPULATE);
        return query;
    }

}
