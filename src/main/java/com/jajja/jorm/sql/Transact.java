package com.jajja.jorm.sql;

import java.sql.Connection;
import java.sql.SQLException;
import com.jajja.jorm.Query;
import com.jajja.jorm.Symbol;
import com.jajja.jorm.Record.ResultMode;

public class Transact extends Sql {

    public static Appender OUTPUT = new Output();

    private static final Appender[] INSERT_APPENDERS =  new Appender[] {
        Standard.INSERT_INTO,
        Transact.OUTPUT,
        Standard.VALUES
    };

    private static final Appender[] UPDATE_APPENDERS =  new Appender[] {
    };

    private static final Appender[] DELETE_APPENDERS =  new Appender[] {
    };

    protected Transact(String database, Connection connection) throws SQLException {
        super(database, connection);
    }

    @Override
    public int getMaxParameters() {
        return 2500;
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

    public static class Output extends Appender {
        @Override
        public void append(Data data, Query query, ResultMode mode) {

            switch (mode) {
            case ID_ONLY:
                query.append(" OUTPUT");
                boolean comma = false;
                for (Symbol symbol : data.table.getPrimaryKey().getSymbols()) {
                    if (comma) {
                        query.append(", INSERTED.#1#", symbol);
                    } else {
                        query.append(" INSERTED.#1#", symbol);
                        comma = true;
                    }
                }
                break;

            case REPOPULATE:
                query.append(" OUTPUT INSERTED.*");
                break;

            case NO_RESULT:
                break;
            }
        }
    }

    // lols

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