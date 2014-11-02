package com.jajja.jorm;

import java.io.Closeable;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import com.jajja.jorm.Record.Field;

public class RecordIterator implements Closeable {
    private Symbol[] symbols;
    private PreparedStatement preparedStatement;
    private ResultSet resultSet;
    private boolean autoClose = true;
    private Calendar calendar;

    public RecordIterator(PreparedStatement preparedStatement) throws SQLException {
        this(preparedStatement, null);
    }

    public RecordIterator(ResultSet resultSet) throws SQLException {
        this(resultSet, null);
    }

    public RecordIterator(PreparedStatement preparedStatement, Calendar calendar) throws SQLException {
        this.preparedStatement = preparedStatement;
        this.calendar = calendar;
        this.resultSet = preparedStatement.executeQuery();
        init();
    }

    public RecordIterator(ResultSet resultSet, Calendar calendar) throws SQLException {
        this.resultSet = resultSet;
        this.calendar = calendar;
        init();
    }

    private void init() throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        symbols = new Symbol[metaData.getColumnCount()];
        for (int i = 0; i < symbols.length; i++) {
            symbols[i] = Symbol.get(metaData.getColumnLabel(i + 1));
        }
    }

    public PreparedStatement getPreparedStatement() {
        return preparedStatement;
    }

    public ResultSet getResultSet() {
        return resultSet;
    }

    private void populate(Record record, ResultSet resultSet) throws SQLException {
        try {
            record.resetFields(symbols.length);
            for (int i = 0; i < symbols.length; i++) {
                Field field = new Record.Field();
                Object object = resultSet.getObject(i + 1);
                if (calendar != null) {
                    if (object instanceof Date) {
                        object = resultSet.getDate(i + 1, calendar);
                    } else if (object instanceof Time) {
                        object = resultSet.getTime(i + 1, calendar);
                    } else if (object instanceof Timestamp) {
                        object = resultSet.getTimestamp(i + 1, calendar);
                    }
                }
                field.setValue(object);
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

    // javadoc: Whether or not to close resultSet + preparedStatement when close() is called
    // (rename to setCascadingClose?)
    public void setAutoClose(boolean autoClose) {
        this.autoClose = autoClose;
    }
}
