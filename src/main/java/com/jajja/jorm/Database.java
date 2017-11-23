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
import java.lang.reflect.Method;
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
import java.util.TimeZone;
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
 * @see Jorm
 * @see Record
 * @author Martin Korinth &lt;martin.korinth@jajja.com&gt;
 * @author Andreas Allerdahl &lt;andreas.allerdahl@jajja.com&gt;
 * @author Daniel Adolfsson &lt;daniel.adolfsson@jajja.com&gt;
 * @since 1.0.0
 */
public class Database {
    private static final Logger logger = Logger.getLogger(Database.class.getName());
    private final ThreadLocal<HashSet<Transaction>> transactions = new ThreadLocal<HashSet<Transaction>>();
    private final Map<String, DataSource> dataSources = new ConcurrentHashMap<String, DataSource>(16, 0.75f, 1);
    private final Map<String, Configuration> configurations = new HashMap<String, Configuration>();
    private static Database instance;

    private Database() {
        __configure();
    }

    /**
     * Acts as singleton factory for bean configuration access. All other access
     * to databases should be static.
     *
     * @return the singleton database representation containing configured data
     * sources for databases.
     */
    public static Database get() {
        if (instance == null) {
            synchronized (Database.class) {
                if (instance == null) {
                    instance = new Database();
                }
            }
        }
        return instance;
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

    public static DataSource getDataSource(String database) {
        return get().dataSources.get(database);
    }

    /**
     * Configures all databases accessible through {@link Database#open(String)}
     * and {@link Database#close(String)}. Overrides any previous configuration.
     *
     * @param dataSources the named databases, each represented by a string and
     * a data source.
     */
    public void setDataSources(Map<String, DataSource> dataSources) {
        this.dataSources.clear();
        this.dataSources.putAll(dataSources);
    }

    /**
     * Configures the named database by means of a data source.
     *
     * @param database the named database.
     * @param dataSource the data source corresponding to the named data base.
     */
    public static void configure(String database, DataSource dataSource) {
        get().__configure(database, dataSource, false);
    }

    private void __configure(String database, DataSource dataSource) {
        __configure(database, dataSource, false);
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
        get().__configure(database,  dataSource, isOverride);
    }

    private void __configure(String database, DataSource dataSource, boolean isOverride) {
        if (!isOverride && __isConfigured(database)) {
            throw new IllegalStateException("Named database '" + database + "' already configured!");
        }
        dataSources.put(database, dataSource);
    }

    /**
     * Determines whether a named database has been configured or not.
     *
     * @param database the named database.
     * @return true if the named database has been configured, false otherwise.
     */
    public static boolean isConfigured(String database) {
        return get().__isConfigured(database);
    }

    private boolean __isConfigured(String database) {
        if (database == null) {
            return false;
        }
        return dataSources.containsKey(database);
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
        get().__assertConfigured(database);
    }

    private void __assertConfigured(String database) {
        if (!__isConfigured(database)) {
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
        DataSource dataSource = getDataSource(database);
        if (dataSource == null) {
            assertConfigured(database); // throws!
        }
        Configuration configuration = get().configurations.get(database);
        try {
            Constructor<T> constructor = clazz.getDeclaredConstructor(DataSource.class, String.class, Calendar.class);
            constructor.setAccessible(true);
            T t = constructor.newInstance(dataSource, database, configuration != null ? configuration.calendar : null);
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

    /*
     * jorm.properties
     * ---------------
     * database.moria.dataSource=org.apache.tomcat.jdbc.pool.DataSource
     * database.moria.dataSource.driverClassName=org.postgresql.Driver
     * database.moria.dataSource.url=jdbc:postgresql://localhost:5432/moria
     * database.moria.dataSource.username=gandalf
     * database.moria.dataSource.password=mellon
     *
     * database.lothlorien.dataSource=org.apache.tomcat.jdbc.pool.DataSource
     * database.lothlorien.dataSource.driverClassName=org.postgresql.Driver
     * database.lothlorien.dataSource.url=jdbc:postgresql://localhost:5432/lothlorien
     * database.lothlorien.dataSource.username=galadriel
     * database.lothlorien.dataSource.password=nenya
     *
     * database.context=
     * database.moria.context=production
     */
    private void __configure() {
        try {
            Enumeration<URL> urls = Thread.currentThread().getContextClassLoader().getResources("jorm.properties");
            List<URL> locals = new LinkedList<URL>();
            while (urls.hasMoreElements()) {
                URL url = urls.nextElement();
                if (url.getProtocol().equals("jar")) {
                    logger.log(Level.FINE, "Found jorm configuration @ " + url.toString());
                    __configure(url);
                } else {
                    locals.add(url);
                }
            }
            for (URL url : locals) {
                logger.log(Level.FINE, "Found jorm configuration @ " + url.toString());
                __configure(url);
            }

            for (Entry<String, Configuration> entry : configurations.entrySet()) {
                String database = entry.getKey();
                Configuration configuration = entry.getValue();
                configuration.init();
                __configure(database, configuration.dataSource);
                Logger.getLogger(Database.class.getName()).log(Level.FINE, "Configured " + configuration);
            }
        } catch (IOException ex) {
            throw new RuntimeException("Failed to configure from jorm.properties", ex);
        }
    }

    public static void configure(URL url) throws IOException {
        Properties properties = new Properties();
        InputStream is = url.openStream();
        properties.load(is);
        is.close();
        get().__configure(properties);
    }

    public static void configure(Properties properties) {
        get().__configure(properties);
    }

    private void __configure(URL url) throws IOException {
        Properties properties = new Properties();
        InputStream is = url.openStream();
        properties.load(is);
        is.close();
        __configure(properties);
    }

    private void __configure(Properties properties) {
        for (Entry<Object, Object> property : properties.entrySet()) {
            String[] parts = ((String)property.getKey()).split("\\.");
            boolean isMalformed = false;

            if (parts[0].equals("database") && parts.length > 1) {
                String database = parts[1];

                Configuration configuration = configurations.get(database);
                if (configuration == null) {
                    configuration = new Configuration(database);
                    configurations.put(database, configuration);
                }

                String value = (String)property.getValue();
                switch (parts.length) {
                case 3:
                    if (parts[2].equals("destroyMethod")) {
                        configuration.destroyMethodName = value;
                    } else if (parts[2].equals("dataSource")) {
                        configuration.dataSourceClassName = value;
                    } else if (parts[2].equals("timeZone")) {
                        if ("default".equalsIgnoreCase(value)) {
                            configuration.calendar = null;
                        } else {
                            configuration.calendar = Calendar.getInstance(TimeZone.getTimeZone(value));
                        }
                    } else {
                        isMalformed = true;
                    }
                    break;

                case 4:
                    if (parts[2].equals("dataSource")) {
                        configuration.dataSourceProperties.put(parts[3], value);
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
    }

    public static class Configuration {
        private final String database;
        private String dataSourceClassName;
        private String destroyMethodName;
        private final Map<String, String> dataSourceProperties = new HashMap<String, String>();
        private DataSource dataSource;
        private Method destroyMethod;
        private Calendar calendar;

        public void inherit(Configuration base) {
            if (dataSourceClassName == null) {
                dataSourceClassName = base.dataSourceClassName;
            }
            if (destroyMethodName == null) {
                destroyMethodName = base.destroyMethodName;
            }
            if (calendar == null) {
                calendar = base.calendar;
            }
            for (String key : base.dataSourceProperties.keySet()) {
                if (!dataSourceProperties.containsKey(key)) {
                    dataSourceProperties.put(key, base.dataSourceProperties.get(key));
                }
            }
        }

        public Configuration(String database) {
            this.database = database;
        }

        private void init() {
            try {
                Class<?> type = Class.forName(dataSourceClassName);
                if (destroyMethodName != null) {
                    try {
                        destroyMethod = type.getMethod(destroyMethodName);
                    } catch (NoSuchMethodException e) {
                        throw new IllegalArgumentException("The destroy method does not exist!", e);
                    } catch (SecurityException e) {
                        throw new IllegalArgumentException("The destroy method is not accessible!", e);
                    }
                }

                dataSource = (DataSource)type.newInstance();
                for (Method method : dataSource.getClass().getMethods()) {
                    String methodName = method.getName();
                    Class<?>[] parameterTypes = method.getParameterTypes();

                    if (methodName.startsWith("set") && methodName.length() > 3 && parameterTypes.length == 1) {
                        String name = method.getName().substring(3, 4).toLowerCase() + method.getName().substring(4); // setValue -> value
                        String property = dataSourceProperties.get(name);
                        if (property != null) {
                            boolean isAccessible = method.isAccessible();
                            method.setAccessible(true);
                            try {
                                method.invoke(dataSource, parse(method.getParameterTypes()[0], property));
                            } catch (Exception e) {
                                throw new RuntimeException("Failed to invoke " + dataSource.getClass().getName() + "#" + method.getName() + "() in configuration of '" + database + "'");
                            } finally {
                                method.setAccessible(isAccessible);
                            }
                        }
                    }
                }
            } catch (InstantiationException e) {
                throw new IllegalArgumentException("The data source implementation " + dataSourceClassName + " has no default constructor!", e);
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException("The data source implementation " + dataSourceClassName + " has no public constructor!", e);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("The data source implementation " + dataSourceClassName + " does not exist!", e);
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("The data source implementation " + dataSourceClassName + " is not a data source!", e);
            }
        }

        public void destroy() {
            if (destroyMethod != null) {
                try {
                    destroyMethod.invoke(dataSource);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to invoke destroy method for " + dataSource.getClass(), e);
                }
            }
        }

        @SuppressWarnings("unchecked")
        private static <T extends Object> T parse(Class<T> type, String property) {
            Object object;
            if (type.isAssignableFrom(String.class)) {
                object = property;
            } else if (type.isAssignableFrom(boolean.class) || type.isAssignableFrom(Boolean.class)) {
                object = Boolean.parseBoolean(property);
            } else if (type.isAssignableFrom(int.class) || type.isAssignableFrom(Integer.class)) {
                object = Integer.parseInt(property);
            } else if (type.isAssignableFrom(long.class) || type.isAssignableFrom(Long.class)) {
                object = Long.parseLong(property);
            } else {
                object = null;
            }
            return (T)object;
        }

        @Override
        public String toString() {
            return "{ database => " + database + ", dataSourceClassName => " + dataSourceClassName + ", dataSourceProperties => " + dataSourceProperties + " }";
        }
    }
}
