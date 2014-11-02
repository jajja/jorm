package com.jajja.jorm;

import java.io.Closeable;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import com.jajja.jorm.Row.Column;

public class RecordIterator implements Closeable {
    private Symbol[] symbols;
    private ResultSet resultSet;
    private PreparedStatement preparedStatement;
    private boolean cascadingClose = true;
    private Transaction transaction;

    /**
     * Instantiates a RecordIterator.
     *
     * @param transaction a Jorm Transaction or null (used to re-throw SQLExceptions and perform TimeZone conversion)
     * @param resultSet a resultSet
     * @throws SQLException
     */
    public RecordIterator(Transaction transaction, ResultSet resultSet) throws SQLException {
        this.resultSet = resultSet;
        this.transaction = transaction;
        init();
    }

    /**
     * Instantiates a RecordIterator.
     *
     * @param transaction a Jorm Transaction or null (used to re-throw SQLExceptions and perform TimeZone conversion)
     * @param preparedStatement a preparedStatement
     * @throws SQLException
     */
    public RecordIterator(Transaction transaction, PreparedStatement preparedStatement) throws SQLException {
        this.preparedStatement = preparedStatement;
        this.resultSet = preparedStatement.executeQuery();
        this.transaction = transaction;
        init();
    }

    private void init() throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        symbols = new Symbol[metaData.getColumnCount()];
        for (int i = 0; i < symbols.length; i++) {
            symbols[i] = Symbol.get(metaData.getColumnLabel(i + 1));
        }
    }

    public ResultSet getResultSet() {
        return resultSet;
    }

    public PreparedStatement getPreparedStatement() {
        return preparedStatement;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public void populate(Row row) throws SQLException {
        try {
            row.resetColumns(symbols.length);
            for (int i = 0; i < symbols.length; i++) {
                Column field = new Record.Column();
                Object object = resultSet.getObject(i + 1);
                if (transaction != null && transaction.getCalendar() != null) {
                    if (object instanceof Date) {
                        object = resultSet.getDate(i + 1, transaction.getCalendar());
                    } else if (object instanceof Time) {
                        object = resultSet.getTime(i + 1, transaction.getCalendar());
                    } else if (object instanceof Timestamp) {
                        object = resultSet.getTimestamp(i + 1, transaction.getCalendar());
                    }
                }
                field.setValue(object);
                row.columns.put(symbols[i], field);
            }
        } catch (SQLException sqlException) {
            row.stale(true);
            if (transaction != null) {
                transaction.getDialect().rethrow(sqlException);
            } else if (preparedStatement != null) {
                throw new Dialect("?", preparedStatement.getConnection()).rethrow(sqlException);
            } else {
                throw sqlException;
            }
        }
    }

    public boolean next() throws SQLException {
        return resultSet.next();
    }

    public <T extends Record> T record(Class<T> clazz) throws SQLException {
        T record = Record.construct(clazz);
        populate(record);
        return record;
    }

    public Row row() throws SQLException {
        Row row = new Row();
        populate(row);
        return row;
    }

    @Override
    public void close() {
        if (!cascadingClose) {
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
    public void setCascadingClose(boolean cascadingClose) {
        this.cascadingClose = cascadingClose;
    }
}
