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
package com.jajja.jorm;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * A code generator for {@link Jorm} mapped records.
 *
 * @see Record
 * @author Andreas Allerdahl <andreas.allerdahl@jajja.com>
 * @author Martin Korinth <martin.korinth@jajja.com>
 * @author Daniel Johansson <daniel.johansson@jajja.com>
 * @since 1.0.0
 */
public class Generator {

    private static HashMap<Integer, String> typeMap = new HashMap<Integer, String>();
    static {
        typeMap.put(java.sql.Types.BIT,             "Boolean");
        typeMap.put(java.sql.Types.TINYINT,         "Integer");
        typeMap.put(java.sql.Types.BOOLEAN,         "Boolean");
        typeMap.put(java.sql.Types.SMALLINT,        "Integer");
        typeMap.put(java.sql.Types.INTEGER,         "Integer");
        typeMap.put(java.sql.Types.BIGINT,          "Long");
        typeMap.put(java.sql.Types.FLOAT,           "Float");
        typeMap.put(java.sql.Types.REAL,            "Float");
        typeMap.put(java.sql.Types.DOUBLE,          "Double");
        typeMap.put(java.sql.Types.NUMERIC,         "BigDecimal");
        typeMap.put(java.sql.Types.DECIMAL,         "BigDecimal");
        typeMap.put(java.sql.Types.CHAR,            "String");
        typeMap.put(java.sql.Types.VARCHAR,         "String");
        typeMap.put(java.sql.Types.LONGVARCHAR,     "String");
        typeMap.put(java.sql.Types.DATE,            "java.sql.Date");
        typeMap.put(java.sql.Types.TIME,            "java.sql.Time");
        typeMap.put(java.sql.Types.TIMESTAMP,       "java.sql.Timestamp");
        typeMap.put(java.sql.Types.BINARY,          "byte[]");
        typeMap.put(java.sql.Types.VARBINARY,       "byte[]");
        typeMap.put(java.sql.Types.LONGVARBINARY,   "byte[]");
        typeMap.put(java.sql.Types.NCHAR,           "String");
        typeMap.put(java.sql.Types.NVARCHAR,        "String");
        typeMap.put(java.sql.Types.LONGNVARCHAR,    "String");
    }

    private String database;
    private String srcPath;
    private String packageName;

    public Generator(String database) {
        this.database = database;
    }

    public Generator file(String srcPath) throws ClassNotFoundException {
        final StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        final StackTraceElement ste = stackTrace[2];
        final String className = ste.getClassName();
        Package p = Class.forName(className).getPackage();

        setSrcPath(srcPath);
        this.packageName = p.getName();
        return this;
    }

    public Generator file(String srcPath, String packageName) {
        setSrcPath(srcPath);
        this.packageName = packageName;
        return this;
    }

    private void setSrcPath(String srcPath) {
        if (!srcPath.endsWith("/")) {
            srcPath += "/";
        }
        this.srcPath = srcPath;
    }

    public String string(String table) throws SQLException {
        return string(table, null);
    }

    public String string(String table, String schema) throws SQLException {
        return generate(table, schema).get("content");
    }

    public void print(String table) throws SQLException {
        print(table, null);
    }

    public void print(String table, String schema) throws SQLException {
        System.out.println(generate(table, schema).get("content"));
    }

    public void write(String table) throws SQLException, IOException {
        write(table, null, false);
    }

    public void write(String table, String schema) throws SQLException, IOException {
        write(table, schema, false);
    }

    public void write(String table, boolean overwrite) throws SQLException, IOException {
        write(table, null, overwrite);
    }

    public void write(String table, String schema, boolean overwrite) throws SQLException, IOException {
        Map<String, String> map = generate(table, schema);
        String filename = map.get("name") + ".java";
        String actualFilename = write(filename, srcPath + getPackagePath(), map.get("content"), overwrite);
        System.out.println("Wrote to " + actualFilename);
    }

    private Map<String, String> generate(String table, String schema) throws SQLException {
        StringBuilder stringBuilder = new StringBuilder();
        Connection c = Database.open(database).getConnection();

        String id = "id";
        ResultSet resultSet = null;

        if (packageName != null) {
            stringBuilder.append("package ");
            stringBuilder.append(packageName);
            stringBuilder.append(";\n");
            stringBuilder.append("\n");
        }

        stringBuilder.append("import com.jajja.jorm.Jorm;\n");
        stringBuilder.append("import com.jajja.jorm.Record;\n");
        stringBuilder.append("\n");

        try {
            resultSet = c.getMetaData().getPrimaryKeys(null, schema, table);
            if (resultSet.next()) {
                id = resultSet.getString("COLUMN_NAME");
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
                resultSet = null;
            }
        }

        String className = depluralize(camelize(table, true));

        stringBuilder.append("@Jorm(");
        stringBuilder.append("database=\"");
        stringBuilder.append(database);
        stringBuilder.append("\", ");
        if (schema != null) {
            stringBuilder.append("schema=\"");
            stringBuilder.append(schema);
            stringBuilder.append("\", ");
        }
        stringBuilder.append("table=\"");
        stringBuilder.append(table);
        stringBuilder.append("\", ");
        stringBuilder.append("id=\"");
        stringBuilder.append(id);
        stringBuilder.append("\")\n");
        stringBuilder.append("public class " + className + " extends Record {\n");

        try {
            resultSet = c.getMetaData().getColumns(null, schema, table, null);
            while (resultSet.next()) {
                String column = resultSet.getString("COLUMN_NAME");
                String typeName = resultSet.getString("TYPE_NAME");
                int dataType = resultSet.getInt("DATA_TYPE");
                String javaDataType = typeMap.get(dataType);

                // MySQL hack.
                if ("INT UNSIGNED".equals(typeName)) {
                    javaDataType = "Long";
                } else if ("BIGINT UNSIGNED".equals(typeName)) {
                    javaDataType = "java.math.BigInteger";
                }

                if (javaDataType == null) javaDataType = "Object";

                String methodName = camelize(column, true);
                String varName = camelize(column, false);

                stringBuilder.append(
                        "    public " + javaDataType + " get" + methodName + "() {\n" +
                                "        return get(\"" + column + "\", " + javaDataType + ".class);\n" +
                                "    }\n" +
                                "\n" +
                                "    public void set" + methodName + "(" + javaDataType + " " + varName + ") {\n" +
                                "        set(\"" + column + "\", " + varName + ");\n" +
                                "    }\n" +
                        "\n");
            }
        } finally {
            if (resultSet != null) resultSet.close();
        }

        stringBuilder.append("}\n");

        Map<String, String> map = new HashMap<String, String>();
        map.put("name", className);
        map.put("content", stringBuilder.toString());
        return map;
    }

    private static String depluralize(String string) { // XXX: more advanced logic?
        return string.endsWith("s") ? string.substring(0, string.length() - 1) : string;
    }

    private static String camelize(String string, boolean upperCase) {
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

    private String getPackagePath() {
        return packageName.replace(".", "/");
    }

    private File getFile(String filename, String filepath, boolean overwrite) throws IOException {
        File destination = new File(filepath);
        if(!destination.exists()) {
            destination.mkdirs();
        }
        File file = new File(destination, filename);
        if (file.exists()) {
            if (overwrite) {
                file.delete();
            } else {
                String[] parts = filename.split("\\.");
                file = getFile(parts[0] + "~." + parts[1], filepath, overwrite);
            }
        }
        if(!file.exists()) {
            file.createNewFile();
        }
        return file;
    }

    private String write(String filename, String filepath, String content, boolean overwrite) throws IOException {
        File file = getFile(filename, filepath, overwrite);
        String actualFilename = file.getPath();
        FileWriter fileWriter = new FileWriter(file, false);
        fileWriter.write(content);
        fileWriter.flush();
        fileWriter.close();
        return actualFilename;
    }

}
