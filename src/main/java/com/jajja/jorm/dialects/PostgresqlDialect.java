package com.jajja.jorm.dialects;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.HashMap;

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
    }

    @Override
    public boolean isReturningSupported() {
        return isReturningSupported;
    }

    @Override
    public boolean isRowWiseComparisonSupported() {
        return true;
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
}
