package com.jajja.jorm.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;

import com.jajja.jorm.Query;
import com.jajja.jorm.Record;
import com.jajja.jorm.Symbol;
import com.jajja.jorm.Table;
import com.jajja.jorm.Record.ResultMode;
import com.jajja.jorm.Row.Column;
import com.jajja.jorm.dialects.Dialect;

public abstract class Sql extends Dialect {

    public abstract int getMaxParameters();
    public abstract Appender[] getAppenders(Operation operation);
    public abstract String getCurrentDateExpression();
    public abstract String getCurrentTimeExpression();
    public abstract String getCurrentDatetimeExpression();

    protected Sql(String database, Connection connection) throws SQLException {
        super(database, connection);
    }

    protected static enum Operation {
        INSERT,
        UPDATE,
        DELETE
    }

    public static class Data {

        int parameters;
        Table table;
        Set<Symbol> pkSymbols;
        Set<Symbol> changedSymbols;
        List<Record> records;

        public Data() {
            parameters = 0;
            changedSymbols = new HashSet<Symbol>();
            records = new LinkedList<Record>();
        }

        public Data(int parameters, Set<Symbol> symbols) {
            this.parameters = parameters;
            this.changedSymbols = symbols;
            records = new LinkedList<Record>();
        }

        public void add(Data impression) {
            this.parameters += impression.parameters;
            this.changedSymbols.addAll(impression.changedSymbols);
            this.records.addAll(impression.records);
        }

        public void add(Record record) {
            if (table != null) {
                table = record.table();
                pkSymbols = new HashSet<Symbol>();
                for (Symbol symbol : table.getPrimaryKey().getSymbols()) {
                    pkSymbols.add(symbol);
                }
            } else if (table != record.table()) {
                throw new IllegalStateException(String.format("Mixed tables in batch! (%s != %s)", table, record.table()));
            }
            records.add(record);
        }

        public boolean isEmpty() {
            return records.isEmpty();
        }

    }

    public static class Batch {

        private final Query query;
        private final Data data;

        Batch(Data data, Query query) {
            this.data = data;
            this.query = query;
        }

        public Table getTable() {
            return data.table;
        }

        public List<Record> getRecords() {
            return data.records;
        }

        public Query getQuery() {
            return query;
        }

    }

    public static abstract class Appender {

        public abstract void append(Data data, Query query, ResultMode mode);

    }

    public Iterator<Batch> batchInsert(List<Record> records, ResultMode mode) {
        return batch(records, mode, Operation.INSERT);
    }

    public Iterator<Batch> batchUpdate(List<Record> records, ResultMode mode) {
        return batch(records, mode, Operation.UPDATE);
    }

    public Iterator<Batch> batchDelete(List<Record> records, ResultMode mode) {
        return batch(records, mode, Operation.DELETE);
    }

    private Iterator<Batch> batch(List<Record> records, ResultMode mode, Operation operation) {
        List<Batch> batches = new LinkedList<Batch>();
        Data data = new Data();
        for (Record record : records) {
            Data increment = getData(record, operation);
            if (data.parameters + increment.parameters < getMaxParameters()) {
                data.add(increment);
            } else {
                batches.add(build(data, mode, operation));
                data = increment;
            }
            data.add(record);
        }
        if (!data.isEmpty()) {
            batches.add(build(data, mode, operation));
        }
        return batches.iterator();
    }

    private Batch build(Data data, ResultMode mode, Operation operation) {
        Query query = new Query(this);
        for (Appender appender : getAppenders(operation)) {
            appender.append(data, query, mode);
        }
        return new Batch(data, query);
    }

    protected Data getData(Record record, Operation type) {
        switch(type) {
        case INSERT:
            return getInsertImpression(record);
        case UPDATE:
            return getUpdateImpression(record);
        case DELETE:
            return getDeleteImpression(record);
        default:
            throw new IllegalStateException(String.format("The batch type %s is unknown!", type));
        }
    }

    protected Data getInsertImpression(Record record) {
        Set<Symbol> symbols = imprint(record);
        return new Data(symbols.size(), symbols);
    }

    protected Data getUpdateImpression(Record record) {
        Set<Symbol> symbols = imprint(record);
        return new Data(record.table().getPrimaryKey().size() + symbols.size(), symbols);
    }

    protected Data getDeleteImpression(Record record) {
        return new Data(record.table().getPrimaryKey().size(), null);
    }

    private Set<Symbol> imprint(Record record) {
        Set<Symbol> symbols = new HashSet<Symbol>();
        for (Entry<Symbol, Column> e : record.columns().entrySet()) {
            if (e.getValue().isChanged()) {
                symbols.add(e.getKey());
            }
        }
        return symbols;
    }

    @Override // XXX remove?
    public ReturnSetSyntax getReturnSetSyntax() {
        // TODO Auto-generated method stub
        return null;
    }

}
