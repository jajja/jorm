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

import static com.jajja.jorm.generator.Generator.camelize;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
public class TableGenerator implements Lookupable {
    private final SchemaGenerator schema;
    private String name;
    private final List<ColumnGenerator[]> uniqueKeys = new LinkedList<ColumnGenerator[]>();
    private final Map<String, ColumnGenerator> columns = new LinkedHashMap<String, ColumnGenerator>();
    private ColumnGenerator primaryColumn;
    private final ImportCollection imports;

    public TableGenerator(SchemaGenerator schema, String name) {
        this.schema = schema;
        this.name = name;
        this.imports = new ImportCollection(schema.getPackageName());
    }

    public Generator getGenerator() {
        return schema.getGenerator();
    }

    public DatabaseGenerator getDatabase() {
        return schema.getDatabase();
    }

    public SchemaGenerator getSchema() {
        return schema;
    }

    public String getName() {
        return name;
    }

    public TableGenerator setName(String name) {
        this.name = name;
        return this;
    }

    public ColumnGenerator getPrimaryColumn() {
        return primaryColumn;
    }

    public TableGenerator setPrimaryColumn(ColumnGenerator primaryColumn) {
        this.primaryColumn = primaryColumn;
        return this;
    }

    public ColumnGenerator getColumn(String name) {
        ColumnGenerator column = columns.get(name);
        if (column == null) {
            throw new NullPointerException("Column " + name + " not found");
        }
        return column;
    }

    public TableGenerator addImport(String importName) {
        imports.add(importName);
        return this;
    }

    public ImportCollection getImports() {
        return imports;
    }

    @SuppressWarnings("resource")
    public void fetchMetadata(Transaction transaction) throws SQLException {
        Connection connection = transaction.getConnection();
        ResultSet resultSet = null;
        String primaryColumnName = "id";

        addImport("com.jajja.jorm.Jorm");
        addImport("com.jajja.jorm.Record");

        try {
            resultSet = connection.getMetaData().getPrimaryKeys(null, schema.getName(), name);
            if (resultSet.next()) {
                primaryColumnName = resultSet.getString("COLUMN_NAME");
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
                resultSet = null;
            }
        }
        try {
            resultSet = connection.getMetaData().getColumns(null, schema.getName(), name, null);
            while (resultSet.next()) {
                ColumnGenerator column = new ColumnGenerator(this, resultSet);
                if (column.getName().equals(primaryColumnName)) {
                    primaryColumn = column;
                    column.setPrimary(true);
                }
                columns.put(column.getName(), column);
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (connection != null) {
                connection.close();
            }
        }

    }

    private static String depluralize(String string) { // XXX: more advanced logic?
        return string.endsWith("s") ? string.substring(0, string.length() - 1) : string;
    }

    public String getClassName() {
        return depluralize(schema.getTablePrefix() + camelize(name, true));
    }

    public String getFullClassName() {
        return schema.getPackageName() + "." + depluralize(schema.getTablePrefix() + camelize(name, true));
    }

    @Override
    public String toString() {
        getDatabase().getGenerator().assertMetadataFetched();

        StringBuilder stringBuilder = new StringBuilder();

//        if (primaryColumn == null) {
//            throw new IllegalStateException("table " + name + " has no primary key");
//        }

        // Package name
        stringBuilder.append("package ").append(schema.getPackageName()).append(";\n");
        stringBuilder.append("\n");

        // Imports
        imports.complete();
        stringBuilder.append(imports);
        stringBuilder.append("\n");

        // Annotation
        stringBuilder.append("@Jorm(");
        stringBuilder.append("database=\"").append(getDatabase().getName()).append("\"");
        if (schema.getName() != null) {
            stringBuilder.append(", schema=\"").append(schema.getName()).append("\"");
        }
        stringBuilder.append(", table=\"").append(name).append("\"");
        if (primaryColumn != null) {
            stringBuilder.append(", primaryKey=\"").append(primaryColumn.getName()).append("\"");
        }
        stringBuilder.append(")\n");

        // The class code
        boolean first = true;
        stringBuilder.append("public class " + getClassName() + " extends Record {\n");
        for (ColumnGenerator columnGenerator : columns.values()) {
            if (!first) {
                stringBuilder.append("\n");
            } else {
                first = false;
            }
            stringBuilder.append(columnGenerator.toString());
        }

        // Unique keys
        for (ColumnGenerator[] columns : uniqueKeys) {
            StringBuilder paramsList = new StringBuilder();
            StringBuilder whereList = new StringBuilder();
            StringBuilder argsList = new StringBuilder();
            String methodName = "findBy";

            for (ColumnGenerator column : columns) {
                methodName += camelize(column.getName(), true);
            }

            int i = 0;
            for (ColumnGenerator column : columns) {
                if (i > 0) {
                    paramsList.append(", ");
                    whereList.append(" AND ");
                    argsList.append(", ");
                }
                paramsList.append(imports.getShortestClassName(column.getJavaDataType())).append(' ').append(camelize(column.getName(), false));
                whereList.append(String.format("%s = #%d#", column.getName(), i + 2));       // XXX escape identifier
                argsList.append(camelize(column.getName(), false));
                i++;
            }

            stringBuilder.append("\n");
            stringBuilder.append(String.format("    public static %s %s(%s) throws SQLException {\n", getClassName(), methodName, paramsList.toString()));

            stringBuilder.append(
                String.format("        return find(%s.class, \"SELECT * FROM #1# WHERE %s\", %s.class, %s);\n",
                    getClassName(),
                    whereList.toString(),
                    getClassName(),
                    argsList.toString()
                )
            );

            stringBuilder.append("    }\n");
        }


        stringBuilder.append("}\n");

        return stringBuilder.toString();
    }

    public void writeFile(String pathname) throws IOException {
        File file = new File(pathname);
        if (!file.createNewFile()) {
            throw new IOException("File " + pathname + " already exists!");
        }
        write(file);
    }

    public void write(File file) throws IOException {
        FileOutputStream os = new FileOutputStream(file);
        try {
            write(os);
        } finally {
            os.close();
        }
    }

    public void write(FileOutputStream os) throws IOException {
        os.write(toString().getBytes("UTF-8"));
    }

    public TableGenerator addUnqiue(ColumnGenerator ... columns) {
        addImport("java.sql.SQLException");     // find() methods throws SQLExceptions
        uniqueKeys.add(columns);
        return this;
    }

    public TableGenerator addUnqiue(String ... columns) {
        ColumnGenerator[] cols = new ColumnGenerator[columns.length];

        for (int i = 0; i < columns.length; i++) {
            cols[i] = getColumn(columns[i]);
        }
        addUnqiue(cols);
        return this;
    }

    @Override
    public Lookupable lookup(List<String> path, Class<? extends Lookupable> expectedClass) {
        ColumnGenerator column = getColumn(path.remove(0));
        if (path.isEmpty()) {
            Generator.assertSameClass(ColumnGenerator.class, expectedClass);
            return column;
        }
        return column.lookup(path, expectedClass);
    }
}
