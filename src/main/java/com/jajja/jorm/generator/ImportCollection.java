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
        add("java.lang", "Object");
        add("java.lang", "Boolean");
        add("java.lang", "Byte");
        add("java.lang", "Short");
        add("java.lang", "Integer");
        add("java.lang", "Long");
        add("java.lang", "Float");
        add("java.lang", "Double");
        add("java.lang", "String");
        add("java.lang", "Long");
    }

    public void add(String packageName, String className) {
        String fullClassName = packageName + "." + className;
        if (imports.containsKey(fullClassName)) {
            return;
        }
        Import i = new Import(packageName, className);
        imports.put(fullClassName, i);
    }

    public void add(String importName) {
        int i = importName.lastIndexOf('.');
        if (i <= 0) {
            throw new IllegalArgumentException("illegal importName: " + importName);
        }
        add(importName.substring(0, i), importName.substring(i + 1));
    }

    public void complete() {
        Set<String> shortImports = new HashSet<String>();
        // Classes in the same package
        for (Import i : imports.values()) {
            if (i.getPackageName().equals(packageName)) {
                shortImports.add(i.getClassName());
            }
        }
        // The rest
        for (Import i : imports.values()) {
            if (i.getPackageName().equals(packageName)) {
                continue;       // already processed
            }
            if (!shortImports.contains(i.getClassName())) {
                shortImports.add(i.getClassName());
                i.setImported(true);
            }
        }
    }

    public String getShortestClassName(String fullClassName) {
        Import i = imports.get(fullClassName);
        if (i == null) {
            throw new IllegalArgumentException("No such class name " + fullClassName);
        }
        return i.isImported() ? i.getClassName() : fullClassName;
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

        @Override
        public String toString() {
            if (getPackageName().equals("java.lang")) {
                return "";
            }
            return String.format("import %s.%s;\n", getPackageName(), getClassName());
        }

        @Override
        public int compareTo(Import i) {
            int cmp = packageName.compareTo(i.getPackageName());
            if (cmp == 0) {
                cmp = className.compareTo(i.getClassName());
            }
            return cmp;
        }
    }
}
