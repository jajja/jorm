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

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.Calendar;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;

/**
 * The database abstraction implemented for {@link Jorm} mapped records. Relies
 * on {@link javax.sql.DataSource} for data access to configured data bases. One
 * recommended implementation for easy configuration is
 * <tt>org.apache.commons.dbcp.BasicDataSource</tt> from the Apache project
 * <tt>commons-dbcp</tt>.
 *
 * @see Configuration
 * @author Martin Korinth &lt;martin.korinth@jajja.com&gt;
 * @author Andreas Allerdahl &lt;andreas.allerdahl@jajja.com&gt;
 * @author Daniel Adolfsson &lt;daniel.adolfsson@jajja.com&gt;
 * @since 1.0.0
 */
public class Database {
    private static final Logger logger = Logger.getLogger(Database.class.getName());
    private final ThreadLocal<HashSet<Transaction>> transactions = new ThreadLocal<HashSet<Transaction>>();
    private final Map<String, Configuration> configurations = new ConcurrentHashMap<String, Configuration>(16, 0.75f, 1);

    private static Database that;

    private Database() {
        // empty!
    }

    /**
     * Configures all databases accessible through {@link Database#open(String)} and
     * {@link Database#close(String)}. Overrides any previous configuration.
     * Intended for configuration in bean instantiation.
     *
     * @param dataSources
     *            the named databases, each represented by a string and a data
     *            source.
     */
    public void setDataSources(Map<String, DataSource> dataSources) {
        configurations.clear();
        for (Entry<String, DataSource> entry : dataSources.entrySet()) {
            configurations.put(entry.getKey(), Configuration.get(entry.getValue()));
        }
    }

    /**
     * Acts as singleton factory for bean configuration access, see
     * #{@link #setDataSources(Map)}.
     *
     * @return the singleton database representation containing configured data
     *         sources for databases.
     */
    public static Database get() {
        if (that == null) {
            synchronized (Database.class) {
                if (that == null) {
                    that = new Database();
                    load();
                }
            }
        }
        return that;
    }

    public static void load() {
        try {
            Enumeration<URL> urls = Thread.currentThread().getContextClassLoader().getResources("jorm.properties");
            List<URL> locals = new LinkedList<URL>();
            while (urls.hasMoreElements()) {
                URL url = urls.nextElement();
                if (url.getProtocol().equals("jar")) {
                    logger.log(Level.FINE, "Found jorm configuration @ " + url.toString());
                    configure(url);
                } else {
                    locals.add(url);
                }
            }
            for (URL url : locals) {
                logger.log(Level.FINE, "Found jorm configuration @ " + url.toString());
                configure(url, true);
            }
        } catch (IOException ex) {
            throw new RuntimeException("Failed to configure from jorm.properties", ex);
        }
    }

    public static void configure(String database, Configuration configuration, boolean isOverride) {
        if (isConfigured(database)) {
            if (isOverride) {
                getConfiguration(database).destroy();
            } else {
                throw new IllegalStateException("Named database '" + database + "' already configured!");
            }
        }
        get().configurations.put(database, configuration);
    }

    public static void configure(String database, Configuration configuration) {
        configure(database, configuration, false);
    }

    /**
     * Configures the named database by means of a data source.
     *
     * @param database the named database.
     * @param dataSource the data source corresponding to the named data base.
     * @param isOverride a flag defining configuration as override if a current
     * configuration for the named database already exists.
     */
    public static void configure(String database, DataSource dataSource, boolean isOverride) {
        configure(database, Configuration.get(dataSource), isOverride);
    }

    /**
     * Configures the named database by means of a data source.
     *
     * @param database the named database.
     * @param dataSource the data source corresponding to the named data base.
     */
    public static void configure(String database, DataSource dataSource) {
        configure(database, dataSource, false);
    }

    public static void configure(Properties properties) {
        configure(properties, false);
    }

    public static void configure(Properties properties, boolean isOverride) {
        Map<String, Properties> confs = new HashMap<String, Properties>();
        for (Entry<Object, Object> property : properties.entrySet()) {
            String[] parts = ((String)property.getKey()).split("\\.");
            boolean isMalformed = false;

            if (parts[0].equals("database") && parts.length > 1) {
                String database = parts[1];

                Properties conf = confs.get(database);
                if (conf == null) {
                    conf = new Properties();
                    confs.put(database, conf);
                }

                String value = (String) property.getValue();
                switch (parts.length) {
                case 3:
                    if (parts[2].equals("destroyMethod") || parts[2].equals("dataSource") || parts[2].equals("timeZone")) {
                        conf.setProperty(parts[2], value);
                    } else {
                        isMalformed = true;
                    }
                    break;

                case 4:
                    if (parts[2].equals("dataSource")) {
                        conf.setProperty("dataSource." + parts[3], value);
                    } else {
                        isMalformed = true;
                    }
                    break;

                default:
                    isMalformed = true;
                }
            } else {
                isMalformed = true;
            }

            if (isMalformed) {
                throw new RuntimeException("Malformed jorm property: " + property.toString());
            }
        }
        for (Entry<String, Properties> entry : confs.entrySet()) {
            configure(entry.getKey(), Configuration.get(entry.getValue()), isOverride);
        }
    }

    public static void configure(URL url) throws IOException {
        configure(url, false);
    }

    public static void configure(URL url, boolean isOverride) throws IOException {
        Properties properties = new Properties();
        InputStream is = url.openStream();
        properties.load(is);
        is.close();
        configure(properties, isOverride);
    }

    private HashSet<Transaction> getThreadLocalTransactions() {
        if (transactions.get() == null) {
            transactions.set(new HashSet<Transaction>());
        }
        return transactions.get();
    }

    private static void register(Transaction transaction) {
        HashSet<Transaction> set = get().getThreadLocalTransactions();
        if (set.contains(transaction)) {
            throw new IllegalStateException("Transaction registered twice?!");
        }
        set.add(transaction);
    }

    static void unregister(Transaction transaction) {
        HashSet<Transaction> set = get().getThreadLocalTransactions();
        if (!set.contains(transaction)) {
            throw new IllegalStateException("Transactions must be closed from the same thread they were created!");
        }
        set.remove(transaction);
    }

    public static Configuration getConfiguration(String database) {
        return get().configurations.get(database);
    }

    public static DataSource getDataSource(String database) {
        return getConfiguration(database).getDataSource();
    }

    /**
     * Determines whether a named database has been configured or not.
     *
     * @param database the named database.
     * @return true if the named database has been configured, false otherwise.
     */
    public static boolean isConfigured(String database) {
        return getConfiguration(database) != null;
    }

    /**
     * Ensures that a named database is configured by throwing an illegal state
     * exception if it is not.
     *
     * @param database the named database.
     * @throws IllegalStateException when the named database has not been
     * configured.
     */
    public static void assertConfigured(String database) {
        if (!isConfigured(database)) {
            throw new IllegalStateException("Named database '" + database + "' has no configured data source!");
        }
    }

    /**
     * Opens a transaction for the given database name.
     *
     * @param database the name of the database.
     * @return the open transaction.
     */
    public static Transaction open(String database) {
        return open(Transaction.class, database);
    }

    public static Set<Transaction> transactions() {
        return Collections.unmodifiableSet(get().getThreadLocalTransactions());
    }

    public static <T extends Transaction> T open(Class<T> clazz, String database) {
        Configuration configuration = getConfiguration(database);
        if (configuration == null) {
            assertConfigured(database); // throws!
        }
        try {
            Constructor<T> constructor = clazz.getDeclaredConstructor(DataSource.class, String.class, Calendar.class);
            constructor.setAccessible(true);
            T t = constructor.newInstance(configuration.getDataSource(), database, configuration.getCalendar());
            register(t);
            return t;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Closes and destroys all transactions for the current thread.
     */
    public static void close() {
        Set<Transaction> set = new HashSet<Transaction>(get().getThreadLocalTransactions());
        for (Transaction t : set) {
            t.close();
        }
        get().transactions.remove();
    }

    public static void destroy() {
        if (get().configurations != null) {
            for (Configuration configuration : get().configurations.values()) {
                configuration.destroy();
            }
        }
    }

}
