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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.jajja.jorm.Database;
import com.jajja.jorm.Dialect;
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
public class DatabaseGenerator implements Lookupable {
    private String packageName;
    private String name;
    private String defaultSchemaName = null;
    private Map<String, String> typeMap = new HashMap<String, String>();
    private Map<String, SchemaGenerator> schemas = new LinkedHashMap<String, SchemaGenerator>();
    private Generator generator;

    public static enum TypeClass {
        ENUM;
    }

    public DatabaseGenerator(Generator generator, String name, String packageName) throws SQLException {
        this.generator = generator;
        this.name = name;
        this.packageName = packageName;
        Transaction transaction = Database.open(name);
        try {
            if (Dialect.DatabaseProduct.POSTGRESQL.equals(transaction.getDialect().getDatabaseProduct())) {
                defaultSchemaName = "public";
            }
            addSchema(defaultSchemaName);
        } finally {
            transaction.close();
        }
    }

    public Generator getGenerator() {
        return generator;
    }

    public String getPackageName() {
        return packageName;
    }

    public String getName() {
        return name;
    }

    public SchemaGenerator addSchema(String name) {
        SchemaGenerator schemaGenerator = new SchemaGenerator(this, name);
        schemas.put(name, schemaGenerator);
        return schemaGenerator;
    }

//    public DatabaseGenerator addAllSchemas(String defaultSchemaName) throws SQLException {
//        Transaction transaction = com.jajja.jorm.Database.open(name);
//        Connection connection = transaction.getConnection();
//        DatabaseMetaData metadata = connection.getMetaData();
//
//        ResultSet rs = null;
//        try {
//            rs = metadata.getSchemas();
//            while (rs.next()) {
//                String schemaName = rs.getString("TABLE_SCHEM");
//                if (defaultSchemaName == null || !defaultSchemaName.equals(schemaName)) {
//                    addSchema(schemaName);
//                }
//            }
//        } finally {
//            if (rs != null) {
//                rs.close();
//            }
//        }
//        return this;
//    }

    public SchemaGenerator getSchema(String schemaName) {
        SchemaGenerator schema = schemas.get(schemaName);
        if (schema == null) {
            throw new NullPointerException("Schema " + schemaName + " not found");
        }
        return schema;
    }

    public SchemaGenerator getDefaultSchema() {
        return schemas.get(defaultSchemaName);
    }

    // Add table to default schema
    public DatabaseGenerator addTable(String tableName) {
        schemas.get(defaultSchemaName).addTable(tableName);
        return this;
    }

    // Add tables to default schema
    public DatabaseGenerator addTables(String ... tableNames) {
        for (String tableName : tableNames) {
            addTable(tableName);
        }
        return this;
    }

    // Add tables from default schema
    public DatabaseGenerator addAllTables() throws SQLException {
        getDefaultSchema().addAllTables();
        return this;
    }

    // Get table from default schema
//    public TableGenerator getTable(String name) {
//        return schemas.get(defaultSchemaName).getTable(name);
//    }

    public String getJavaDataType(String databaseTypeName) {
        return typeMap.get(databaseTypeName);
    }

    public DatabaseGenerator mapDataType(String databaseTypeName, String javaTypeName) {
        getGenerator().assertMetadataNotFetched();
        typeMap.put(databaseTypeName, javaTypeName);
        return this;
    }

    public void fetchMetadata() throws SQLException {
        Transaction transaction = Database.open(name);

        try {
            if (transaction.getDialect().getDatabaseProduct().equals(Dialect.DatabaseProduct.MYSQL)) {
                mapDataType("INT UNSIGNED", "java.lang.Long");
                mapDataType("BIGINT UNSIGNED", "java.math.BigInteger");
            }
            if (transaction.getDialect().getDatabaseProduct().equals(Dialect.DatabaseProduct.POSTGRESQL)) {
                mapDataType("box", "org.postgresql.geometric.PGbox");
                mapDataType("circle", "org.postgresql.geometric.PGcircle");
                mapDataType("interval", "org.postgresql.util.PGInterval");
                mapDataType("line", "org.postgresql.geometric.PGline");
                mapDataType("lseg", "org.postgresql.geometric.PGlseg");
                mapDataType("money", "org.postgresql.util.PGmoney");
                mapDataType("path", "org.postgresql.geometric.PGpath");
                mapDataType("point", "org.postgresql.geometric.PGpoint");
                mapDataType("polygon", "org.postgresql.geometric.PGpolygon");

                mapDataType("uuid", "java.util.UUID");
            }
            for (SchemaGenerator schema : schemas.values()) {
                schema.fetchMetadata(transaction);
            }
        } finally {
            transaction.close();
        }
    }

    void fetchForeignKeys() throws SQLException {
        Transaction transaction = Database.open(name);

        try {
            // Fetch foreign keys
            for (SchemaGenerator schema : schemas.values()) {
                for (TableGenerator table : schema.tables.values()) {
                    ResultSet resultSet = transaction.getConnection().getMetaData().getImportedKeys(null, schema.getName(), table.getName());
                    try {
                        while (resultSet.next()) {
                            // table = offices
                            // PK* = null.public.companies.id
                            // FK* = null.public.offices.company_id
                            ColumnGenerator column = table.getColumn(resultSet.getString("FKCOLUMN_NAME"));
                            SchemaGenerator pkSchema = null;
                            TableGenerator pkTable = null;
                            ColumnGenerator pkColumn = null;

                            if (column == null) {
                                // Should not happen
                                continue;
                            }

                            pkSchema = getSchema(resultSet.getString("PKTABLE_SCHEM"));
                            if (pkSchema != null) {
                                pkTable = pkSchema.getTable(resultSet.getString("PKTABLE_NAME"));
                            }
                            if (pkTable != null) {
                                pkColumn = pkTable.getColumn(resultSet.getString("PKCOLUMN_NAME"));
                            }
                            if (pkColumn != null && pkColumn.isPrimary()) {
                                //System.out.println(String.format("* %s.%s references %s.%s", table.getName(), column.getName(), pkColumn.getTable().getName(), pkColumn.getName()));
                                column.addReference(pkColumn);
                            }
                        }
                    } finally {
                        resultSet.close();
                    }
                }
            }
        } finally {
            transaction.close();
        }
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();

        for (SchemaGenerator schema : schemas.values()) {
            stringBuilder.append(schema.toString());
        }

        return stringBuilder.toString();
    }

    public void writeFiles(String rootPath) throws IOException {
        for (SchemaGenerator schema : schemas.values()) {
            if (schema.isDefaultSchema()) {
                schema.writeFiles(rootPath);
            } else {
                schema.writeFiles(rootPath + File.separator + schema.getName());
            }
        }
    }

    @Override
    public Lookupable lookup(List<String> path, Class<? extends Lookupable> expectedClass) {
        String name = path.remove(0);
        if (name.equals("@")) {
            name = defaultSchemaName;   // default schema
        }
        SchemaGenerator schema = getSchema(name);
        if (path.isEmpty()) {
            Generator.assertSameClass(SchemaGenerator.class, expectedClass);
            return schema;
        }
        return schema.lookup(path, expectedClass);
    }

    public TableGenerator getTable(String path) {
        return (TableGenerator)Generator.lookup(this, path, TableGenerator.class);
    }

    public ColumnGenerator getColumn(String path) {
        return (ColumnGenerator)Generator.lookup(this, path, ColumnGenerator.class);
    }
}
