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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
public class ColumnGenerator implements Lookupable {
    private TableGenerator table;
    private String name;
    private String typeName;
    private Integer dataType;
    private String javaDataType;
    private boolean isPrimary;
    private LinkedList<ColumnGenerator> references = new LinkedList<ColumnGenerator>();

    private static Map<Integer, String> typeMap = new LinkedHashMap<Integer, String>();
    static {
        typeMap.put(java.sql.Types.BIT,             "java.lang.Boolean");
        typeMap.put(java.sql.Types.TINYINT,         "java.lang.Integer");
        typeMap.put(java.sql.Types.BOOLEAN,         "java.lang.Boolean");
        typeMap.put(java.sql.Types.SMALLINT,        "java.lang.Integer");
        typeMap.put(java.sql.Types.INTEGER,         "java.lang.Integer");
        typeMap.put(java.sql.Types.BIGINT,          "java.lang.Long");
        typeMap.put(java.sql.Types.FLOAT,           "java.lang.Float");
        typeMap.put(java.sql.Types.REAL,            "java.lang.Float");
        typeMap.put(java.sql.Types.DOUBLE,          "java.lang.Double");
        typeMap.put(java.sql.Types.NUMERIC,         "java.math.BigDecimal");
        typeMap.put(java.sql.Types.DECIMAL,         "java.math.BigDecimal");
        typeMap.put(java.sql.Types.CHAR,            "java.lang.String");
        typeMap.put(java.sql.Types.VARCHAR,         "java.lang.String");
        typeMap.put(java.sql.Types.LONGVARCHAR,     "java.lang.String");
        typeMap.put(java.sql.Types.DATE,            "java.sql.Date");
        typeMap.put(java.sql.Types.TIME,            "java.sql.Time");
        typeMap.put(java.sql.Types.TIMESTAMP,       "java.sql.Timestamp");
        typeMap.put(java.sql.Types.BINARY,          "byte[]");
        typeMap.put(java.sql.Types.VARBINARY,       "byte[]");
        typeMap.put(java.sql.Types.LONGVARBINARY,   "byte[]");
        typeMap.put(java.sql.Types.NCHAR,           "java.lang.String");
        typeMap.put(java.sql.Types.NVARCHAR,        "java.lang.String");
        typeMap.put(java.sql.Types.LONGNVARCHAR,    "java.lang.String");
    }

    public ColumnGenerator(TableGenerator table, ResultSet resultSet) throws SQLException {
        this.table = table;
        name = resultSet.getString("COLUMN_NAME");
        typeName = resultSet.getString("TYPE_NAME");
        dataType = resultSet.getInt("DATA_TYPE");
        javaDataType = table.getDatabase().getJavaDataType(typeName);
        if (javaDataType == null) {
            javaDataType = typeMap.get(dataType);
        }
        table.addImport(getJavaDataType());
    }

    public Generator getGenerator() {
        return table.getGenerator();
    }

    public DatabaseGenerator getDatabase() {
        return table.getDatabase();
    }

    public SchemaGenerator getSchema() {
        return table.getSchema();
    }

    public TableGenerator getTable() {
        return table;
    }

    public String getName() {
        return name;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public Integer getDataType() {
        return dataType;
    }

    public void setDataType(Integer dataType) {
        this.dataType = dataType;
    }

    public String getJavaDataType() {
        return javaDataType != null ? javaDataType : "java.lang.Object";
    }

    public void setJavaDataType(String javaDataType) {
        this.javaDataType = javaDataType;
    }

    public void setPrimary(boolean isPrimary) {
        this.isPrimary = isPrimary;
    }

    public boolean isPrimary() {
        return isPrimary;
    }

    public void addReference(ColumnGenerator column) {
        getGenerator().assertMetadataFetched();
        if (!column.isPrimary()) {
            throw new UnsupportedOperationException("Referencing non-primary keys is not supported (yet)");
        }
        references.add(column);
        // *Ref() methods throw SQLException
        table.addImport("java.sql.SQLException");
        table.addImport(column.getTable().getFullClassName());
    }

    public void addReference(TableGenerator table, String column) {
        addReference(table.getColumn(column));
    }

    public void addReference(SchemaGenerator schema, String tableName, String columnName) {
        addReference(schema.getTable(tableName), columnName);
    }

    public void addReference(DatabaseGenerator database, String schemaName, String tableName, String columnName) {
        addReference(database.getSchema(schemaName), tableName, columnName);
    }

    public void addReference(String databaseName, String schemaName, String tableName, String columnName) {
        addReference(getGenerator().getDatabase(databaseName), schemaName, tableName, columnName);
    }

    public void addReference(String path) {
        List<String> parts = Arrays.asList(path.split("\\."));
        switch (parts.size()) {
        case 1:
            addReference((ColumnGenerator)Generator.lookup(getTable(), path, ColumnGenerator.class));
            break;
        case 2:
            addReference((ColumnGenerator)Generator.lookup(getSchema(), path, ColumnGenerator.class));
            break;
        case 3:
            addReference((ColumnGenerator)Generator.lookup(getDatabase(), path, ColumnGenerator.class));
            break;
        case 4:
            addReference((ColumnGenerator)Generator.lookup(getGenerator(), path, ColumnGenerator.class));
            break;
        default:
            throw new IllegalArgumentException("Invalid path");
        }
    }

    public static void append(StringBuilder sb, String fmt, Object ... args) {
        sb.append(String.format(fmt, args));
    }

    private static List<Pattern> idStripPatterns = new LinkedList<Pattern>();
    static {
        idStripPatterns.add(Pattern.compile("_id$", Pattern.CASE_INSENSITIVE));
        idStripPatterns.add(Pattern.compile("Id$"));
    }

    public static String stripId(String columnName) {
        for (Pattern pattern : idStripPatterns) {
            Matcher matcher = pattern.matcher(columnName);
            if (matcher.find()) {
                return matcher.replaceFirst("");
            }
        }
        return columnName;
    }

    @Override
    public String toString() {
        getTable().getDatabase().getGenerator().assertMetadataFetched();

        StringBuilder stringBuilder = new StringBuilder();
        String methodName = camelize(name, true);
        String varName = camelize(name, false);

        String javaDataType = table.getImports().getShortestClassName(getJavaDataType());

        append(stringBuilder, "    public %s get%s() {\n", javaDataType, methodName);
        append(stringBuilder, "        return get(\"%s\", %s.class);\n", name, javaDataType);
        append(stringBuilder, "    }\n");
        append(stringBuilder, "\n");
        append(stringBuilder, "    public void set%s(%s %s) {\n", methodName, javaDataType, varName);
        append(stringBuilder, "        set(\"%s\", %s);\n", name, varName);
        append(stringBuilder, "    }\n");

        for (ColumnGenerator column : references) {
            String refName = stripId(name);
            String refMethodName = camelize(refName, true);
            String refVarName = camelize(refName, false);

            String refClassName =  table.getImports().getShortestClassName(column.getTable().getFullClassName());

            append(stringBuilder, "\n");
            append(stringBuilder, "    public %s get%sRef() throws SQLException {\n", column.getTable().getClassName(), refMethodName);
            append(stringBuilder, "        return ref(\"%s\", %s.class);\n", name, refClassName);
            append(stringBuilder, "    }\n");
            append(stringBuilder, "\n");
            append(stringBuilder, "    public void set%sRef(%s %s) {\n", refMethodName, refClassName, refVarName);
            append(stringBuilder, "        set(\"%s\", %s);\n", name, refVarName);
            append(stringBuilder, "    }\n");
        }

        return stringBuilder.toString();
    }

    @Override
    public Lookupable lookup(List<String> path, Class<? extends Lookupable> expectedClass) {
        throw new IllegalArgumentException("Your path contains too many parts");
    }
}
