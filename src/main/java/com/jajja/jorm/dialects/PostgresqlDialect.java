package com.jajja.jorm.dialects;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import com.jajja.jorm.Query;
import com.jajja.jorm.Record;
import com.jajja.jorm.Symbol;
import com.jajja.jorm.Table;
import com.jajja.jorm.Record.ResultMode;


public class PostgresqlDialect extends Dialect {
    private static final HashMap<String, ExceptionType> exceptionMap = new HashMap<String, ExceptionType>();
    private final boolean isReturningSupported;

    static {
        exceptionMap.put("23503", ExceptionType.FOREIGN_KEY_VIOLATION);     // foreign_key_violation
        exceptionMap.put("23505", ExceptionType.UNIQUE_VIOLATION);          // unique_violation
        exceptionMap.put("23514", ExceptionType.CHECK_VIOLATION);           // check_violation
        exceptionMap.put("40P01", ExceptionType.DEADLOCK_DETECTED);         // deadlock_detected
        exceptionMap.put("55P03", ExceptionType.LOCK_TIMEOUT);              // lock_not_available
    }

    PostgresqlDialect(String database, Connection connection) throws SQLException {
        super(database, connection);

        DatabaseMetaData metaData = connection.getMetaData();
        int major = metaData.getDatabaseMajorVersion();
        int minor = metaData.getDatabaseMinorVersion();
        isReturningSupported = (major > 8 || (major == 8 && minor > 1));

        feature(Feature.BATCH_INSERTS);
        feature(Feature.BATCH_UPDATES);
        feature(Feature.ROW_WISE_COMPARISONS);
    }

    @Override
    public ReturnSetSyntax getReturnSetSyntax() {
        return isReturningSupported ? ReturnSetSyntax.RETURNING : ReturnSetSyntax.NONE;
    }

    @Override
    public ExceptionType getExceptionType(SQLException sqlException) {
        ExceptionType type = exceptionMap.get(sqlException.getSQLState());
        return type != null ? type : ExceptionType.UNKNOWN;
    }

    @Override
    public String getNowFunction() {
        return "now()";
    }

    @Override
    public String getNowQuery() {
        return "SELECT now()";
    }

    @Override
    protected void appendInsertTail(Query query, Table table, Set<Symbol> symbols, List<Record> records, ResultMode mode) {
        query.append(" RETURNING");
        if (mode == ResultMode.ID_ONLY) {
            boolean isFirst = true;
            for (Symbol symbol : table.getPrimaryKey().getSymbols()) {
                if (isFirst) {
                    isFirst = false;
                    query.append(" #1#", symbol);
                } else {
                    query.append(", #1#", symbol);
                }
            }
        } else {
            query.append(" *");
        }
    }

    @Override
    public int getMaxParameterMarkers() {
        return 32768;
    }

}
