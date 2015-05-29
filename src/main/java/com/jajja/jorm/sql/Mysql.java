package com.jajja.jorm.sql;

import java.sql.Connection;
import java.sql.SQLException;

public class Mysql extends Standard {

    protected Mysql(String database, Connection connection) throws SQLException {
        super(database, connection);
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

}
