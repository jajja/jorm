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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.jajja.jorm.Jorm;
import com.jajja.jorm.Record;

/**
 * A code generator for {@link Jorm} mapped records.
 *
 * @see Record
 * @author Andreas Allerdahl &lt;andreas.allerdahl@jajja.com&gt;
 * @author Martin Korinth &lt;martin.korinth@jajja.com&gt;
 * @since 1.1.0
 */
public class Generator implements Lookupable {
    private Map<String, DatabaseGenerator> databases = new LinkedHashMap<String, DatabaseGenerator>();
    private boolean metadataFetched = false;

    public Generator() {
    }

    public void fetchMetadata() throws SQLException {
        if (metadataFetched) {
            throw new IllegalStateException("Metadata already fetched");
        }
        for (DatabaseGenerator database : databases.values()) {
            database.fetchMetadata();
        }
        metadataFetched = true;
        for (DatabaseGenerator database : databases.values()) {
            database.fetchForeignKeys();
        }
    }

    public boolean isMetadataFetched() {
        return metadataFetched;
    }

    public void assertMetadataNotFetched() {
        if (metadataFetched) {
            throw new IllegalStateException("Metadata has been fetched!");
        }
    }

    public void assertMetadataFetched() {
        if (!metadataFetched) {
            throw new IllegalStateException("Metadata not fetched!");
        }
    }

    public DatabaseGenerator addDatabase(String name, String packageName) throws SQLException {
        DatabaseGenerator database = new DatabaseGenerator(this, name, packageName);
        databases.put(name, database);
        return database;
    }

    public DatabaseGenerator getDatabase(String name) {
        DatabaseGenerator database = databases.get(name);
        if (database == null) {
            throw new NullPointerException("Database " + name + " not found");
        }
        return database;
    }

    static String camelize(String string, boolean upperCase) {
        StringBuffer stringBuffer = new StringBuffer(string);
        for (int i = 0; i < stringBuffer.length(); i++) {
            if (upperCase && i == 0) {
                stringBuffer.setCharAt(i, Character.toUpperCase(stringBuffer.charAt(i)));
                upperCase = false;
            }
            if (stringBuffer.charAt(i) == '_') {
                stringBuffer.deleteCharAt(i);
                stringBuffer.setCharAt(i, Character.toUpperCase(stringBuffer.charAt(i)));
            }
        }
        return stringBuffer.toString();
    }

    @Override
    public String toString() {
        assertMetadataFetched();
        StringBuilder stringBuilder = new StringBuilder();

        for (DatabaseGenerator database : databases.values()) {
            stringBuilder.append(database.toString());
        }
        return stringBuilder.toString();
    }

    public void writeFiles(String rootPath) throws IOException {
        for (DatabaseGenerator database : databases.values()) {
            database.writeFiles(rootPath + File.separator + database.getName());
        }
    }

    protected static void assertSameClass(Class<?> resultClass, Class<?> expectedClass) {
        if (!resultClass.equals(expectedClass)) {
            String what = "?";
            if (resultClass.equals(DatabaseGenerator.class)) {
                what = "database";
            } else if (resultClass.equals(SchemaGenerator.class)) {
                what = "schema";
            } else if (resultClass.equals(TableGenerator.class)) {
                what = "table";
            } else if (resultClass.equals(ColumnGenerator.class)) {
                what = "column";
            }
            throw new IllegalStateException("Path refers to a " + what);
        }
    }

    @Override
    public Lookupable lookup(List<String> path, Class<? extends Lookupable> expectedClass) {
        DatabaseGenerator database = getDatabase(path.remove(0));
        if (path.isEmpty()) {
            assertSameClass(DatabaseGenerator.class, expectedClass);
            return database;
        }
        return database.lookup(path, expectedClass);
    }

    public static Lookupable lookup(Lookupable rel, String path, Class<? extends Lookupable> expectedClass) {
        return rel.lookup(new LinkedList<String>(Arrays.asList(path.split("\\."))), expectedClass);
    }

    public SchemaGenerator getSchema(String path) {
        return (SchemaGenerator)lookup(this, path, SchemaGenerator.class);
    }

    public TableGenerator getTable(String path) {
        return (TableGenerator)lookup(this, path, TableGenerator.class);
    }

    public ColumnGenerator getColumn(String path) {
        return (ColumnGenerator)lookup(this, path, ColumnGenerator.class);
    }
}
