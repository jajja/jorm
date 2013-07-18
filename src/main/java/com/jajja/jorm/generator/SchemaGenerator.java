/*
 * Copyright (C) 2013 Jajja Communications AB
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
package com.jajja.jorm.generator;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.jajja.jorm.Jorm;
import com.jajja.jorm.Record;
import com.jajja.jorm.Transaction;


/**
 * A code generator for {@link Jorm} mapped records.
 *
 * @see Record
 * @author Andreas Allerdahl <andreas.allerdahl@jajja.com>
 * @author Martin Korinth <martin.korinth@jajja.com>
 * @since 1.1.0
 */
public class SchemaGenerator implements Lookupable {
    private String name;
    private String packageName;
    private Map<String, TableGenerator> tables = new LinkedHashMap<String, TableGenerator>();
    private DatabaseGenerator database;

    public SchemaGenerator(DatabaseGenerator database, String name) {
        this.database = database;
        this.name = name;
        this.packageName = database.getPackageName();
        if (name != null) {
            this.packageName += "." + name.toLowerCase();
        }
    }

    public Generator getGenerator() {
        return database.getGenerator();
    }

    public DatabaseGenerator getDatabase() {
        return database;
    }

    public String getName() {
        return name;
    }

    public boolean isDefaultSchema() {
        return name == null;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPackageName() {
        return packageName;
    }

    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    public TableGenerator getTable(String name) {
        TableGenerator table = tables.get(name);
        if (table == null) {
            throw new NullPointerException("Table " + name + " not found");
        }
        return table;
    }

    public void addTable(String tableName) {
        tables.put(tableName, new TableGenerator(this, tableName));
    }

    public void addTables(String ... tableNames) {
        for (String tableName : tableNames) {
            addTable(tableName);
        }
    }

    public void fetchMetadata(Transaction transaction) throws SQLException {
        for (TableGenerator table : tables.values()) {
            table.fetchMetadata(transaction);
        }
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();

        for (TableGenerator table : tables.values()) {
            stringBuilder.append(table.toString());
        }

        return stringBuilder.toString();
    }

    public void writeFiles(String rootPath) throws IOException {
        new File(rootPath).mkdirs();
        for (TableGenerator table : tables.values()) {
            table.writeFile(rootPath + File.separator + table.getName() + ".java");
        }
    }

    @Override
    public Lookupable lookup(List<String> path, Class<? extends Lookupable> expectedClass) {
        TableGenerator table = getTable(path.remove(0));
        if (path.isEmpty()) {
            Generator.assertSameClass(TableGenerator.class, expectedClass);
            return table;
        }
        return table.lookup(path, expectedClass);
    }

    public ColumnGenerator getColumn(String path) {
        return (ColumnGenerator)Generator.lookup(this, path, ColumnGenerator.class);
    }
}
