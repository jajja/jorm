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

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The database abstraction implemented for {@link Jorm} mapped records. Relies
 * on {@link javax.sql.DataSource} for data access to configured data bases. One
 * recommended implementation for easy configuration is
 * <tt>org.apache.commons.dbcp.BasicDataSource</tt> from the Apache project
 * <tt>commons-dbcp</tt>.
 *
 * @see Jorm
 * @see Record
 * @author Martin Korinth <martin.korinth@jajja.com>
 * @author Andreas Allerdahl <andreas.allerdahl@jajja.com>
 * @author Daniel Adolfsson <daniel.adolfsson@jajja.com>
 * @since 1.0.0
 */
public class Database {
    private ThreadLocal<HashMap<String, Transaction>> transactions = new ThreadLocal<HashMap<String, Transaction>>();
    private Map<String, DataSource> dataSources = new HashMap<String, DataSource>();
    protected Log log = LogFactory.getLog(Database.class);
    private static volatile Database instance = new Database();

    private Database() { }

    /**
     * Acts as singleton factory for bean configuration access. All other access
     * to databases should be static.
     *
     * @return the singleton database representation containing configured data
     *         sources for databases.
     */
    public static synchronized Database get() {
        return instance;
    }

    private HashMap<String, Transaction> getTransactions() {
        if (transactions.get() == null) {
            transactions.set(new HashMap<String, Transaction>());
        }
        return transactions.get();
    }

    private DataSource getDataSource(String database) {
        synchronized (dataSources) {
            return dataSources.get(database);
        }
    }

    /**
     * Configures all databases accessible through {@link Database#open(String)}
     * and {@link Database#close(String)}. Overrides any previous configuration.
     *
     * @param dataSources
     *            the named databases, each represented by a string and a data
     *            source.
     */
    public void setDataSources(Map<String, DataSource> dataSources) {
        this.dataSources = dataSources;
    }

    /**
     * Configures the named database by means of a data source.
     *
     * @param database
     *            the named database.
     * @param dataSource
     *            the data source corresponding to the named data base.
     */
    public static void configure(String database, DataSource dataSource) {
        configure(database, dataSource, false);
    }

    /**
     * Configures the named database by means of a data source.
     *
     * @param database
     *            the named database.
     * @param dataSource
     *            the data source corresponding to the named data base.
     * @param isOverride
     *            a flag defining configuration as override if a current
     *            configuration for the named database already exists.
     */
    public static void configure(String database, DataSource dataSource, boolean isOverride) {
        if (!isOverride && isConfigured(database)) {
            throw new IllegalStateException("Named database '" + database + "' already configured!");
        }
        instance.dataSources.put(database, dataSource);
    }

    /**
     * Determines whether a named database has been configured or not.
     *
     * @param database
     *            the named database.
     * @return true if the named database has been configured, false otherwise.
     */
    public static boolean isConfigured(String database) {  // XXX: read/write lock on data sources?
        return instance.dataSources.containsKey(database);
    }

    /**
     * Ensures that a named database is configured by throwing an illegal state
     * exception if it is not.
     *
     * @param database
     *            the named database.
     * @throws IllegalStateException
     *             when the named database has not been configured.
     */
    public static void ensureConfigured(String database) {
        if (!isConfigured(database)) throw new IllegalStateException("Named database '" + database + "' has no configured data source!");
    }

    /**
     * Opens a thread local transaction for the given database name. If an open
     * transaction already exists, it is reused. This method is idempotent when
     * called from the same thread.
     *
     * @param database
     *            the name of the database.
     * @return the open transaction.
     */
    public static Transaction open(String database) {
        HashMap<String, Transaction> transactions = instance.getTransactions();
        Transaction transaction = transactions.get(database);
        if (transaction == null) {
            DataSource dataSource = instance.getDataSource(database);
            if (dataSource == null) {
                ensureConfigured(database); // throws!
            }
            transaction = new Transaction(dataSource, database);
            transactions.put(database, transaction);
        }
        return transaction;
    }

    /**
     * Commits the thread local transaction for the given database name if it
     * has been opened.
     *
     * @param database
     *            the name of the database.
     * @return the closed transaction or null for no active transaction.
     * @throws SQLException
     *             if a database access error occur
     */
    public static Transaction commit(String database) throws SQLException {
        HashMap<String, Transaction> transactions = instance.getTransactions();
        Transaction transaction = transactions.get(database);
        if (transaction != null) {
            transaction.commit();
        } else {
            ensureConfigured(database);
        }
        return transaction;
    }

    /**
     * Closes the thread local transaction for the given database name if it has
     * been opened. This method is idempotent when called from the same thread.
     *
     * @param database
     *            the name of the database.
     * @return the closed transaction or null for no active transaction.
     */
    public static Transaction close(String database) {
        HashMap<String, Transaction> transactions = instance.getTransactions();
        Transaction transaction = transactions.get(database);
        if (transaction != null) {
            transaction.close();
        } else {
            ensureConfigured(database);
        }
        return transaction;
    }

    /**
     * Closes and destroys all transactions for the current thread.
     */
    public static void close() {
        HashMap<String, Transaction> map = instance.getTransactions();
        for (Transaction transaction : map.values()) {
            transaction.destroy();
        }
        map.clear();
        instance.transactions.remove();
    }
}
