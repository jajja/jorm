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
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.jajja.jorm.Jorm;
import com.jajja.jorm.Record;
import com.jajja.jorm.Transaction;


/**
 * A code generator for {@link Jorm} mapped records.
 *
 * @see Record
 * @author Andreas Allerdahl &lt;andreas.allerdahl@jajja.com&gt;
 * @author Martin Korinth &lt;martin.korinth@jajja.com&gt;
 * @since 1.1.0
 */
public class SchemaGenerator implements Lookupable {
    private static final Set<String> reservedKeywords = new HashSet<String>();
    private String name;
    private String packageName;
    Map<String, TableGenerator> tables = new LinkedHashMap<String, TableGenerator>();
    private DatabaseGenerator database;
    private String tablePrefix = "";

    static {
        String[] keywords = new String[] {
                "abstract", "assert", "boolean", "break", "byte", "case",
                "catch", "char", "class", "const", "continue", "default",
                "do", "double", "else", "enum", "extends", "final",
                "finally", "float", "for", "goto", "if", "implements",
                "import", "instanceof", "int", "interface", "long",
                "native", "new", "package", "private", "protected",
                "public", "return", "short", "static", "strictfp",
                "super", "switch", "synchronized", "this", "throw",
                "throws", "transient", "try", "void", "volatile",
                "while", "false", "null", "true" };
        for (String keyword : keywords) {
            reservedKeywords.add(keyword);
        }
    }

    public SchemaGenerator(DatabaseGenerator database, String name) {
        this.database = database;
        this.name = name;
        this.packageName = database.getPackageName();
        if (name != null) {
            String lcaseName = name.toLowerCase();
            while (reservedKeywords.contains(lcaseName)) {
                lcaseName = "x" + lcaseName;
            }
            this.packageName += "." + lcaseName;
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

    public SchemaGenerator setName(String name) {
        this.name = name;
        return this;
    }

    public String getPackageName() {
        return packageName;
    }

    public SchemaGenerator setPackageName(String packageName) {
        this.packageName = packageName;
        return this;
    }

    public String getTablePrefix() {
        return tablePrefix;
    }

    public SchemaGenerator setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
        return this;
    }

    public TableGenerator getTable(String name) {
        TableGenerator table = tables.get(name);
        if (table == null) {
            throw new NullPointerException("Table " + name + " not found in schema " + getName());
        }
        return table;
    }

    public TableGenerator addTable(String tableName) {
        TableGenerator tableGenerator = new TableGenerator(this, tableName);
        tables.put(tableName, tableGenerator);
        return tableGenerator;
    }

    public SchemaGenerator addTables(String ... tableNames) {
        for (String tableName : tableNames) {
            addTable(tableName);
        }
        return this;
    }

    public SchemaGenerator addAllTables() throws SQLException {
        Transaction transaction = com.jajja.jorm.Database.open(getDatabase().getName());
        Connection connection = transaction.getConnection();
        DatabaseMetaData metadata = connection.getMetaData();

        ResultSet rs = null;
        try {
            String[] types = new String[] {"TABLE", "VIEW"};
            rs = metadata.getTables(null, name, null, types);
            while (rs.next()) {
                addTable(rs.getString("TABLE_NAME"));
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
        }
        return this;
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
            table.writeFile(rootPath + File.separator + table.getClassName() + ".java");
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
