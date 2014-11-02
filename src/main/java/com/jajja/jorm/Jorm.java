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

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotation for specifying mappings of records to database tables. Generic
 * mappings can be specified without given table and primary key. Table and
 * primary key are mutually required if one of them is given. For code example
 * using this annotation, examine {@link Record}.
 *
 * @see Record
 * @see Database
 * @author Martin Korinth <martin.korinth@jajja.com>
 * @author Andreas Allerdahl <andreas.allerdahl@jajja.com>
 * @since 1.0.0
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Jorm {
    /**
     * Name of the database as defined by a named data source.
     *
     * @see Database
     * @return the table name
     */
    public String database();

    /**
     * Name of mapped schema. Use the default value "" for default schema.
     *
     * @return the schema name
     */
    public String schema() default "";

    /**
     * Name of mapped table. Use the default value "" for generic SQL.
     *
     * @return the table name
     */
    public String table() default "";
    /**
     * Name of the columns that make up the primary key. Use the default value "" for generic SQL.
     *
     * @return the primary key column names
     */
    public String[] primaryKey() default {};

    /**
     * A list of names for columns mapped as immutable columns.
     *
     *
     * @return the column names of the immutable columns.
     */
    public String immutablePrefix() default "__";
}
