package com.jajja.jorm;

import java.io.Closeable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.jajja.jorm.Record.Field;

public class RecordIterator implements Closeable {
    private Symbol[] symbols;
    private PreparedStatement preparedStatement;
    private ResultSet resultSet;
    private boolean autoClose = true;

    public RecordIterator(PreparedStatement preparedStatement) throws SQLException {
        this.preparedStatement = preparedStatement;
        this.resultSet = preparedStatement.executeQuery();
        init();
    }

    public RecordIterator(ResultSet resultSet) throws SQLException {
        this.resultSet = resultSet;
        init();
    }

    private void init() throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        symbols = new Symbol[metaData.getColumnCount()];
        for (int i = 0; i < symbols.length; i++) {
            symbols[i] = Symbol.get(metaData.getColumnLabel(i + 1));
        }
    }

    private void populate(Record record, ResultSet resultSet) throws SQLException {
        try {
            record.newFields(symbols.length);
            for (int i = 0; i < symbols.length; i++) {
                Field field = new Record.Field();
                field.setValue(resultSet.getObject(i + 1));
                record.fields.put(symbols[i], field);
            }
        } catch (SQLException sqlException) {
            record.stale(true);
            record.transaction().getDialect().rethrow(sqlException);
        }
    }

    public boolean next() throws SQLException {
        return resultSet.next();
    }

    public <T extends Record> T record(Class<T> clazz) throws SQLException {
        T record = Record.construct(clazz);
        populate(record, resultSet);
        return record;
    }

    public void record(Record record) throws SQLException {
        populate(record, resultSet);
    }

    @Override
    public void close() {
        if (!autoClose) {
            return;
        }
        Exception ex = null;
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (Exception e) {
                ex = e;
            }
            resultSet = null;
        }
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (Exception e) {
                ex = e;
            }
            preparedStatement = null;
        }
        if (ex != null) {
            throw new RuntimeException(ex);
        }
    }

    public void setAutoClose(boolean autoClose) {
        this.autoClose = autoClose;
    }
}
