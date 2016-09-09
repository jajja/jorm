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

import java.util.concurrent.ConcurrentHashMap;

/**
 * A 'multiton' identifier symbol implementation for low memory footprint in the
 * loosely coupled representation of columns names and values in {@link Record}.
 *
 * @see Record
 * @see Field
 * @author Martin Korinth &lt;martin.korinth@jajja.com&gt;
 * @author Andreas Allerdahl &lt;andreas.allerdahl@jajja.com&gt;
 * @since 1.0.0
 */
public final class Symbol implements Comparable<Symbol> {
    private static volatile ConcurrentHashMap<String, Symbol> symbols = new ConcurrentHashMap<String, Symbol>(512, 0.75f, 1);
    final int identity;
    private final String name;

    /**
     * Gets the equality identity and the hashable value of the symbol.
     *
     * @return the identity.
     */
    public int getIdentity() {
        return identity;
    }

    /**
     * Gets the name and content of the symbol.
     *
     * @return the name.
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the single instance of a symbol corresponding to a given name.
     * Populates the symbol iff not previously accessed.
     *
     * @param name
     *            the name.
     * @return the symbol corresponding to the given name.
     */
    public static Symbol get(String name) {
        Symbol symbol = symbols.get(name);

        if (symbol == null) {
            synchronized (symbols) {
                symbol = symbols.get(name);
                if (symbol == null) {
                    symbol = new Symbol(symbols.size() + 1, name);
                    symbols.put(name, symbol);
                }
            }
        }

        return symbol;
    }

    private Symbol(Integer identity, String string) {
        if (string == null || string.isEmpty()) {
            throw new IllegalArgumentException("Symbols cannot have empty content!");
        }
        if (identity < 1) {
            throw new RuntimeException("Something has maliciously created more than " + Integer.MAX_VALUE + " symbols!");
        }
        this.identity = identity;
        this.name = string;
    }

    @Override
    public int hashCode() {
        return identity;
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof Symbol) {
            return ((Symbol)object).identity == identity;
        } else if (object instanceof String) {
            return ((String)object).equals(name);
        }
        return false;
    }

    @Override
    public int compareTo(Symbol o) {
        return (identity < o.identity) ? -1 : ((identity == o.identity) ? 0 : 1);
    }

    @Override
    public String toString() {
        return "Symbol " + identity + ":" + name;
    }
}
