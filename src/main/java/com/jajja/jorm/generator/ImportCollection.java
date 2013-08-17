package com.jajja.jorm.generator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ImportCollection {
    private Map<String, Import> imports = new HashMap<String, Import>();
    private String packageName;

    public ImportCollection(String packageName) {
        this.packageName = packageName;
        add("java.lang.Object");
        add("java.lang.Boolean");
        add("java.lang.Byte");
        add("java.lang.Short");
        add("java.lang.Integer");
        add("java.lang.Long");
        add("java.lang.Float");
        add("java.lang.Double");
        add("java.lang.String");
        add("java.lang.Long");
    }

    public void add(String importName) {
        Import i = new Import(importName);
        if (imports.containsKey(i.getFullClassName())) {
            return;
        }
        imports.put(i.getFullClassName(), i);
    }

    private static boolean isPrimitive(String name) {
        String[] types = new String[] { "byte", "short", "int", "long", "float", "double", "boolean", "char" };
        for (String type : types) {
            if (type.equals(name)) {
                return true;
            }
        }
        return false;
    }

    public void complete() {
        Set<String> shortImports = new HashSet<String>();
        // Classes in the same package
        for (Import i : imports.values()) {
            if (packageName.equals(i.getPackageName())) {
                shortImports.add(i.getClassName());
                i.setImported(true);
            }
        }
        // The rest
        for (Import i : imports.values()) {
            if (packageName.equals(i.getPackageName())) {
                continue;       // already processed
            }
            if (!shortImports.contains(i.getClassName())) {
                shortImports.add(i.getClassName());
                i.setImported(true);
            }
        }
    }

    public String getShortestClassName(String importName) {
        Import i = new Import(importName);
        if (i.isPrimitive()) {
            return importName;
        }
        Import i2 = imports.get(i.getFullClassName());
        if (i2 == null) {
            throw new IllegalArgumentException("No such class name " + i.getFullClassName());
        }
        importName = i2.isImported() ? i2.getClassName() : i2.getFullClassName();
        if (i2.isArray()) {
            importName += "[]";
        }
        return importName;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        List<Import> values = new ArrayList<Import>(imports.values());

        Collections.sort(values);
        for (Import i : values) {
            if (i.isImported()) {
                stringBuilder.append(i);
            }
        }

        return stringBuilder.toString();
    }

    public static class Import implements Comparable<Import> {
        private String packageName;
        private String className;
        private boolean imported;
        private boolean isPrimitive = false;
        private boolean isArray = false;

        public Import(String importName) {
            if (importName.endsWith("[]")) {
                importName = importName.substring(0, importName.length() - 2);
                this.isArray = true;
            }
            isPrimitive = ImportCollection.isPrimitive(importName);
            int i = importName.lastIndexOf('.');
            if (i <= 0) {
                this.className = importName;
            } else {
                this.packageName = importName.substring(0, i);
                this.className = importName.substring(i + 1);
            }
        }

        public Import(String packageName, String className) {
            this.packageName = packageName;
            this.className = className;
        }

        public String getPackageName() {
            return packageName;
        }

        public void setPackageName(String packageName) {
            this.packageName = packageName;
        }

        public String getClassName() {
            return className;
        }

        public void setClassName(String className) {
            this.className = className;
        }

        public void setImported(boolean imported) {
            this.imported = imported;
        }

        public boolean isImported() {
            return imported;
        }

        public boolean isPrimitive() {
            return isPrimitive;
        }

        public boolean isArray() {
            return isArray;
        }

        public String getFullClassName() {
            if (packageName == null) {
                return className;
            } else {
                return packageName + "." + className;
            }
        }

        @Override
        public String toString() {
            if (packageName == null || packageName.equals("java.lang")) {
                return "";
            }
            return String.format("import %s.%s;\n", getPackageName(), getClassName());
        }

        @Override
        public int compareTo(Import i) {
            int cmp = 0;
            if (i.getPackageName() == packageName) {
                cmp = 0;
            } else if (packageName == null) {
                cmp = -1;
            } else {
                cmp = packageName.compareTo(i.getPackageName());
            }
            if (cmp == 0) {
                cmp = className.compareTo(i.getClassName());
            }
            return cmp;
        }
    }
}
