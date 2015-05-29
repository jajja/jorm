package com.jajja.jorm.sql;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.regex.Pattern;

import com.jajja.jorm.Query;
import com.jajja.jorm.Symbol;
import com.jajja.jorm.Record.ResultMode;

public class Transact extends Standard {

    private final static Pattern FOREIGN_KEY_VIOLATION_PATTERN = Pattern.compile("^The [A-Z ]+ statement conflicted with the FOREIGN KEY constraint");

    private static final HashMap<Integer, ExceptionType> EXCEPTIONS = new HashMap<Integer, ExceptionType>();
    static {
//      EXCEPTIONS.put(547, ExceptionType.FOREIGN_KEY_VIOLATION); // %ls statement conflicted with %ls %ls constraint '%.*ls'. The conflict occurred in database '%.*ls', table '%.*ls'%ls%.*ls%ls.
        EXCEPTIONS.put(2601, ExceptionType.UNIQUE_VIOLATION);     // Cannot insert duplicate key row in object '%.*ls' with unique index '%.*ls'.
        EXCEPTIONS.put(2627, ExceptionType.UNIQUE_VIOLATION);     // Violation of %ls constraint '%.*ls'. Cannot insert duplicate key in object '%.*ls'.
        EXCEPTIONS.put(547, ExceptionType.CHECK_VIOLATION);       // %ls statement conflicted with %ls %ls constraint '%.*ls'. The conflict occurred in database '%.*ls', table '%.*ls'%ls%.*ls%ls.
        EXCEPTIONS.put(1205, ExceptionType.DEADLOCK_DETECTED);    // Transaction (Process ID %d) was deadlocked on {%Z} resources with another process and has been chosen as the deadlock victim. Rerun the transaction.
        EXCEPTIONS.put(1222, ExceptionType.LOCK_TIMEOUT);         // Lock request time out period exceeded.
    }

    public static Appender OUTPUT = new Output();

    private static final Appender[] INSERT_APPENDERS =  new Appender[] {
        Standard.INSERT_INTO,
        Transact.OUTPUT,
        Standard.VALUES
    };

    private final boolean isReturning;

    protected Transact(Product product) throws SQLException {
        super(product);
        isReturning = product.getMajor() >= 2006;
    }

    @Override
    public int getMaxParameters() {
        return 2500;
    }

    @Override
    public boolean isReturningSupported() {
        return isReturning;
    }

    // https://msdn.microsoft.com/en-us/library/ms188751.aspx

    @Override
    public String getCurrentDateExpression() {
        return "CAST(getdate() AS date)";
    }

    @Override
    public String getCurrentTimeExpression() {
        return "CAST(getdate() AS time)";
    }

    @Override
    public String getCurrentDatetimeExpression() {
        return "getdate()";
    }

    @Override
    public ExceptionType getExceptionType(SQLException sqlException) {
        ExceptionType type = EXCEPTIONS.get(sqlException.getErrorCode());
        if (ExceptionType.CHECK_VIOLATION.equals(type)) {
            if (FOREIGN_KEY_VIOLATION_PATTERN.matcher(sqlException.getMessage()).matches()) {
                type = ExceptionType.FOREIGN_KEY_VIOLATION;
            }
        }
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

}
