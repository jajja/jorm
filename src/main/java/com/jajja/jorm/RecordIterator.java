/*
 * Copyright (C) 2014 Jajja Communications AB
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
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
import com.jajja.jorm.sql.Language;

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
                Column column = row.createColumn(symbols[i]);
                column.setValue(object);
            }
        } catch (SQLException sqlException) {
            row.stale(true);
            if (transaction != null) {
                transaction.getLanguage().rethrow(sqlException);
            } else if (preparedStatement != null) {
                throw Language.get(preparedStatement.getConnection()).rethrow(sqlException);
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
