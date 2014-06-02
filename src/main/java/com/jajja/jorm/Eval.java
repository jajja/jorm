package com.jajja.jorm;

public class Eval extends Composite {

    public Eval(String ... columns) {
        if (columns.length == 0) {
            throw new IllegalArgumentException("At least 1 column is required");
        }
        this.symbols = new Symbol[columns.length];
        for (int i = 0; i < columns.length; i++) {
            this.symbols[i] = Symbol.get(columns[i], true);
        }
    }
}
