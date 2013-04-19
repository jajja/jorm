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

/**
 * <p>
 * A convenience construction which supports the need to query for records by
 * given columns of the mapped table. Porvides the structure for implementing
 * composite keys.
 * </p>
 * <h3>Example queries</h3>
 * <p>
 * Columns are used in the
 * {@link Record#find(Class, Column...)} and {@link Record#findAll(Class, Column...)} queries.
 * <pre>
 * Record.find(Locale.class, new Column(&quot;language&quot;, &quot;sv&quot;), new Column(&quot;country&quot;, &quot;SE&quot;))
 * Record.findAll(Locale.class, new Column(&quot;language&quot;, &quot;en&quot;))
 * </pre>
 * </p>
 * 
 * <h3>Composite key implementation</h3>
 * <p>
 * Composite keys can be implemented by wrapping {@link Record#find(Class, Column...)}.
 * </p>
 * <p>
 * <strong>From SQL:</strong>
 * <pre>
 * CREATE TABLE locales (
 *     id        serial    NOT NULL,
 *     language  varchar   NOT NULL,
 *     country   varchar,
 *     PRIMARY KEY (id),
 *     UNIQUE (language, country)
 * )
 * </pre>
 * 
 * </p>
 * <p>
 * <strong>To Java:</strong>
 * <pre>
 * &#064;Table(database=&quot;default&quot;, table=&quot;locales&quot;, id=&quot;id&quot;)
 * public class Locale extends Record {
 * 
 *     public static Locale findByCompositeKey(String language, String country) {
 *         return Record.find(
 *             Locale.class,
 *             new Column(&quot;language&quot;, language),
 *             new Column(&quot;country&quot;, country)
 *         );
 *     }
 * 
 *     public Integer getId() {
 *         return get(&quot;id&quot;, Integer.class);
 *     }
 * 
 *     public void setId(Integer id) {
 *         set(&quot;id&quot;, id);
 *     }
 * 
 *     public String getLanguage() {
 *         return get(&quot;language&quot;, String.class);
 *     }
 * 
 *     public void setLanguage(String language) {
 *         set(&quot;language&quot;, language);
 *     }
 * 
 *     public String getCountry() {
 *         return get(&quot;country&quot;, String.class);
 *     }
 * 
 *     public void setCountry(String country) {
 *         set(&quot;country&quot;, country);
 *     }
 * 
 *     public java.util.Locale getLocale() {
 *         return new java.util.Locale(getLanguage(), getCountry());
 *     }
 * 
 * }
 * </pre>
 * </p>
 * 
 * @see Record
 * @author Martin Korinth <martin.korinth@jajja.com>
 * @since 1.0.0
 */
public class Column {
    private Symbol symbol;
    private Object value;

    /**
     * Constructs a column for queries by given name and value.
     * 
     * @param name
     *            the name.
     * @param value
     *            the value.
     */
    public Column(String name, Object value) {
        this.symbol = Symbol.get(name);
        this.value = value;
    }

    /**
     * Constructs a column for queries by given symbol and value.
     * 
     * @param symbol
     *            the symbol representing the name of the column.
     * @param value
     *            the value.
     */
    public Column(Symbol symbol, Object value) {
        this.symbol = symbol;
        this.value = value;
    }

    /**
     * Provides the symbol representing the name of the column.
     * 
     * @return the symbol.
     */
    public Symbol getSymbol() {
        return symbol;
    }


    /**
     * Defines the symbol representing the name of the column.
     * 
     * @param symbol
     *            the symbol.
     */
    public void setSymbol(Symbol symbol) {
        this.symbol = symbol;
    }

    /**
     * Provides the value of the column.
     * 
     * @return the value.
     */
    public Object getValue() {
        return value;
    }


    /**
     * Defines the value of the column.
     * 
     * @param value
     *            the value.
     */
    public void setValue(Object value) {
        this.value = value;
    }
}
