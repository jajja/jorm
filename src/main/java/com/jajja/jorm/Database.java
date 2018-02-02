/*
 * Copyright (C) 2018 Jajja Communications AB
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

import java.lang.reflect.Constructor;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.sql.DataSource;

/**
 * The database abstraction implemented for {@link Jorm} mapped records. Relies
 * on {@link javax.sql.DataSource} for data access to configured data bases. One
 * recommended implementation for easy configuration is
 * <tt>org.apache.commons.dbcp.BasicDataSource</tt> from the Apache project
 * <tt>commons-dbcp</tt>.
 *
 * @see Transaction
 * @see Configuration
 * @see Configurations
 * @author Martin Korinth &lt;martin.korinth@jajja.com&gt;
 * @author Andreas Allerdahl &lt;andreas.allerdahl@jajja.com&gt;
 * @author Daniel Adolfsson &lt;daniel.adolfsson@jajja.com&gt;
 * @since 1.0.0
 */
public class Database {
    private final String database;
    private final Configuration configuration;
    private final ThreadLocal<HashSet<Transaction>> transactions = new ThreadLocal<HashSet<Transaction>>();

    public Database(String database) {
        this.database = database;
        this.configuration = Configurations.getConfiguration(database);
        if (configuration == null) {
            throw new IllegalArgumentException(database + " is not configured");
        }
    }

    public Database(String database, Configuration configuration) {
        Configurations.configure(database, configuration);
        this.database = database;
        this.configuration = configuration;
    }

    private HashSet<Transaction> getThreadLocalTransactions() {
        if (transactions.get() == null) {
            transactions.set(new HashSet<Transaction>());
        }
        return transactions.get();
    }

    private void register(Transaction transaction) {
        HashSet<Transaction> set = getThreadLocalTransactions();
        if (set.contains(transaction)) {
            throw new IllegalStateException("Transaction registered twice?!");
        }
        set.add(transaction);
    }

    void unregister(Transaction transaction) {
        HashSet<Transaction> set = getThreadLocalTransactions();
        if (!set.contains(transaction)) {
            throw new IllegalStateException("Transactions must be closed from the same thread they were created!");
        }
        set.remove(transaction);
    }

    public String getDatabase() {
        return database;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public DataSource getDataSource() {
        return configuration.getDataSource();
    }

    /**
     * Opens a transaction for the given database name.
     *
     * @param database the name of the database.
     * @return the open transaction.
     */
    public Transaction open() {
        return open(Transaction.class);
    }

    public Set<Transaction> transactions() {
        return Collections.unmodifiableSet(getThreadLocalTransactions());
    }

    public <T extends Transaction> T open(Class<T> clazz) {
        try {
            Constructor<T> constructor = clazz.getDeclaredConstructor(Database.class, DataSource.class, Calendar.class);
            constructor.setAccessible(true);
            T t = constructor.newInstance(this, configuration.getDataSource(), configuration.getCalendar());
            register(t);
            return t;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Closes and destroys all transactions for the current thread.
     */
    public void close() {
        Set<Transaction> set = new HashSet<Transaction>(getThreadLocalTransactions());
        for (Transaction transaction : set) {
            transaction.close();
        }
        transactions.remove();
    }
}
