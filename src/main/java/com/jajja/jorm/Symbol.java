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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * A 'multiton' identifier symbol implementation for low memory footprint in the
 * loosely coupled representation of columns names and values in {@link Record}.
 * 
 * @see Record
 * @see Column
 * @author Martin Korinth <martin.korinth@jajja.com>
 * @author Andreas Allerdahl <andreas.allerdahl@jajja.com>
 * @since 1.0.0
 */
public final class Symbol {
    private static volatile Map<String, Symbol> symbols = new HashMap<String, Symbol>();
    private final Integer identity;
    private final String name;

    /**
     * Gets the equality identity and the hashable value of the symbol.
     * 
     * @return the identity.
     */
    public Integer getIdentity() {
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
        if (symbol != null) {
            return symbol;
        }

        synchronized (Symbol.class) {
            symbol = symbols.get(name);
            if (symbol != null) {
                return symbol;
            }

            Map<String, Symbol> symbolsCopy = new HashMap<String, Symbol>();
            for (Entry<String, Symbol> entry : symbols.entrySet()) {
                symbolsCopy.put(entry.getKey(), entry.getValue());
            }

            int identity = size() + 1;
            symbol = new Symbol(identity, name);
            symbolsCopy.put(name, symbol);
            symbols = symbolsCopy;

            return symbol;
        }
    }
    
    /**
     * Gets the size of the entire symbol dictionary.
     * 
     * @return the size.
     */
    public static int size() {
        return symbols.size();
    }
    
    private Symbol(Integer identity, String string) {
        if (string == null) {
            throw new IllegalArgumentException("Symbols cannot have null content!");
        }
        if (identity < 1) {
            throw new RuntimeException("Something has maliciously created more than " + Integer.MAX_VALUE + " symbols!");
        }
        this.identity = identity;
        this.name = string;
    }

    @Override
    public int hashCode() {
        return identity.hashCode();
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof Symbol) {
            return ((Symbol)object).identity == identity;
        } else if (object instanceof String) {
            return ((String) object).equals(name);
        }
        return false;
    }
    
}
