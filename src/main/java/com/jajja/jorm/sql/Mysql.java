package com.jajja.jorm.sql;

import java.sql.SQLException;
import java.util.HashMap;

public class Mysql extends Standard {

    private static final HashMap<Integer, ExceptionType> EXCEPTIONS = new HashMap<Integer, ExceptionType>();
    static {
        EXCEPTIONS.put(1216, ExceptionType.FOREIGN_KEY_VIOLATION);    // SQLSTATE: 23000 (ER_NO_REFERENCED_ROW) Cannot add or update a child row: a foreign key constraint fails
        EXCEPTIONS.put(1217, ExceptionType.FOREIGN_KEY_VIOLATION);    // SQLSTATE: 23000 (ER_ROW_IS_REFERENCED) Cannot delete or update a parent row: a foreign key constraint fails
        EXCEPTIONS.put(630, ExceptionType.UNIQUE_VIOLATION);          // Tuple already existed when attempting to insert
        EXCEPTIONS.put(893, ExceptionType.UNIQUE_VIOLATION);          // Constraint violation e.g. duplicate value in unique index
        EXCEPTIONS.put(1062, ExceptionType.UNIQUE_VIOLATION);         // SQLSTATE: 23000 (ER_DUP_ENTRY) Duplicate entry '%s' for key %d
        EXCEPTIONS.put(1213, ExceptionType.DEADLOCK_DETECTED);        // SQLSTATE: 40001 (ER_LOCK_DEADLOCK) Deadlock found when trying to get lock; try restarting transaction
        EXCEPTIONS.put(1205, ExceptionType.LOCK_TIMEOUT);             // SQLSTATE: HY000 (ER_LOCK_WAIT_TIMEOUT) Lock wait timeout exceeded; try restarting transaction
    }

    protected Mysql(Product product) throws SQLException {
        super(product);
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
    public ExceptionType getExceptionType(SQLException sqlException) {
        ExceptionType type = EXCEPTIONS.get(sqlException.getErrorCode());
        return type != null ? type : ExceptionType.UNKNOWN;
    }

}
