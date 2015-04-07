package com.jajja.jorm.dialects;

import java.sql.Connection;
import java.sql.SQLException;

public class GenericDialect extends Dialect {
    GenericDialect(String database, Connection connection) throws SQLException {
        super(database, connection);
    }

    @Override
    public ReturnSetSyntax getReturnSetSyntax() {
        return ReturnSetSyntax.NONE;
    }

    @Override
    public boolean isRowWiseComparisonSupported() {
        return false;
    }

    @Override
    public ExceptionType getExceptionType(SQLException sqlException) {
        return ExceptionType.UNKNOWN;
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
