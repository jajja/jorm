package com.jajja.jorm.dialects;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;

import com.jajja.jorm.Record;
import com.jajja.jorm.Symbol;
import com.jajja.jorm.Table;
import com.jajja.jorm.Row.Column;

public class Batch {

    public static enum Type {
        INSERT,
        UPDATE,
        DELETE
    }

    final Dialect dialect;
    final Table table;
    final Type type;

    private int parameterMarkers = 0;
    private List<Record> records = new LinkedList<Record>();
    private Set<Symbol> symbols = new LinkedHashSet<Symbol>();

    Batch(Dialect dialect, Table table, Type type) {
        this.dialect = dialect;
        this.table = table;
        this.type = type;
    }

    public boolean enter(Record record) throws SQLException {
        if (table != record.table()) {
            throw new SQLException("Cannot change different tables in the same batch!");
        }
        int parameterMarkers = 0;
        Set<Symbol> symbols = null;

        switch(type) {
        case INSERT:
            symbols = getSymbols(record);
            parameterMarkers = dialect.getInsertParmaterMarkers(table, symbols);
            break;
        case UPDATE:
            symbols = getSymbols(record);
            parameterMarkers = dialect.getUpdateParmaterMarkers(table, symbols);
            break;
        case DELETE:
            parameterMarkers = dialect.getDeleteParmaterMarkers(table);
            break;
        default:
            throw new IllegalStateException(String.format("The batch type %s is unknown!", type));
        }

        if (this.parameterMarkers + parameterMarkers < dialect.getMaxParameterMarkers()) {
            this.parameterMarkers += parameterMarkers;
            records.add(record);
            if (symbols != null) {
                this.symbols.addAll(symbols);
            }
            return true;
        } else {
            return false;
        }
    }

    private Set<Symbol> getSymbols(Record record) {
        Set<Symbol> symbols = new HashSet<Symbol>();
        for (Entry<Symbol, Column> e : record.columns().entrySet()) {
            if (e.getValue().isChanged()) {
                symbols.add(e.getKey());
            }
        }
        return symbols;
    }

    public List<Record> getRecords() {
        return records;
    }

    public Set<Symbol> getSymbols() {
        return symbols;
    }

    public Iterator<Record> iterator() {
        return records.iterator();
    }

}
