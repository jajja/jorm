package com.jajja.jorm.dialects;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;


public class MysqlDialect extends Dialect {
    private static final HashMap<Integer, ExceptionType> exceptionMap = new HashMap<Integer, ExceptionType>();

    static {
        exceptionMap.put(1216, ExceptionType.FOREIGN_KEY_VIOLATION);    // SQLSTATE: 23000 (ER_NO_REFERENCED_ROW) Cannot add or update a child row: a foreign key constraint fails
        exceptionMap.put(1217, ExceptionType.FOREIGN_KEY_VIOLATION);    // SQLSTATE: 23000 (ER_ROW_IS_REFERENCED) Cannot delete or update a parent row: a foreign key constraint fails
        exceptionMap.put(630, ExceptionType.UNIQUE_VIOLATION);          // Tuple already existed when attempting to insert
        exceptionMap.put(893, ExceptionType.UNIQUE_VIOLATION);          // Constraint violation e.g. duplicate value in unique index
        exceptionMap.put(1062, ExceptionType.UNIQUE_VIOLATION);         // SQLSTATE: 23000 (ER_DUP_ENTRY) Duplicate entry '%s' for key %d
        exceptionMap.put(1213, ExceptionType.DEADLOCK_DETECTED);        // SQLSTATE: 40001 (ER_LOCK_DEADLOCK) Deadlock found when trying to get lock; try restarting transaction
        exceptionMap.put(1205, ExceptionType.LOCK_TIMEOUT);             // SQLSTATE: HY000 (ER_LOCK_WAIT_TIMEOUT) Lock wait timeout exceeded; try restarting transaction
    }

    MysqlDialect(String database, Connection connection) throws SQLException {
        super(database, connection);
        feature(Feature.BATCH_INSERTS);
    }

    @Override
    public ReturnSetSyntax getReturnSetSyntax() {
        return ReturnSetSyntax.NONE;
    }

    @Override
    public Object getPrimaryKeyValue(ResultSet resultSet, int column) throws SQLException {
        return resultSet.getLong(column);
    }

    @Override
    public ExceptionType getExceptionType(SQLException sqlException) {
        ExceptionType type = exceptionMap.get(sqlException.getErrorCode());
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
