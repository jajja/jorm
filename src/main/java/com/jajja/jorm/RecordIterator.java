package com.jajja.jorm;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class RecordIterator /*implements AutoCloseable*/ {      // XXX how to support AutoCloseable in Java < 1.7?
    private Symbol[] symbols;
    private Set<Symbol> symbolSet = new HashSet<Symbol>();
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
        symbolSet = new HashSet<Symbol>(symbols.length + 1, 1.0f);    // + 1 to prevent resize?
        for (int i = 0; i < symbols.length; i++) {
            symbols[i] = Symbol.get(metaData.getColumnLabel(i + 1));
            symbolSet.add(symbols[i]);
        }
    }

    private void populate(Record record, ResultSet resultSet) throws SQLException {
        for (int i = 0; i < symbols.length; i++) {
            record.isStale = false;
            try {
                record.put(symbols[i], resultSet.getObject(i + 1));
            } catch (SQLException sqlException) {
                record.open().getDialect().rethrow(sqlException);
            } finally {
                record.isStale = true; // lol exception
            }
            record.isStale = false;
        }
        Iterator<Symbol> i = record.fields.keySet().iterator();
        while (i.hasNext()) {
            Symbol symbol = i.next();
            if (!symbolSet.contains(symbol)) {
                record.unset(symbol);
            }
        }
        record.purify();
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

    //@Override
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
