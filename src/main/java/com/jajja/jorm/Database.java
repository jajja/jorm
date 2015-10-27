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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

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
    // FIXME daz alotta hashmapz...
    private final ThreadLocal<HashMap<String, Transaction>> transactions = new ThreadLocal<HashMap<String, Transaction>>();
    private final ThreadLocal<HashMap<String, Context>> contextStack = new ThreadLocal<HashMap<String, Context>>();
    private final Map<String, DataSource> dataSources = new ConcurrentHashMap<String, DataSource>(16, 0.75f, 1);
    private final Map<String, Configuration> configurations = new HashMap<String, Configuration>();
    private final Map<String, String> defaultContext = new HashMap<String, String>();
    private String globalDefaultContext = "";
    private static Database instance;           // NEVER use directly, always use get()

    private Database() {
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
                    instance.configure();
                }
            }
        }
        return instance;
    }

    private HashMap<String, Transaction> getTransactions() {
        if (transactions.get() == null) {
            transactions.set(new HashMap<String, Transaction>());
        }
        return transactions.get();
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
        configure(database, dataSource, false);
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
        if (!isOverride && isConfigured(database)) {
            throw new IllegalStateException("Named database '" + database + "' already configured!");
        }
        get().dataSources.put(database, dataSource);
    }

    /**
     * Determines whether a named database has been configured or not.
     *
     * @param database the named database.
     * @return true if the named database has been configured, false otherwise.
     */
    public static boolean isConfigured(String database) {
        if (database == null) {
            return false;
        }
        return get().dataSources.containsKey(database);
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
     * Opens a thread local transaction for the given database name. If an open
     * transaction already exists, it is reused. This method is idempotent when
     * called from the same thread.
     *
     * @param database the name of the database.
     * @return the open transaction.
     */
    public static Transaction open(String database) {
        database = context(database).effectiveName();
        HashMap<String, Transaction> transactions = get().getTransactions();
        Transaction transaction = transactions.get(database);
        if (transaction == null) {
            DataSource dataSource = getDataSource(database);
            if (dataSource == null) {
                assertConfigured(database); // throws!
            }
            Configuration configuration = get().configurations.get(database);
            transaction = new Transaction(dataSource, database, configuration != null ? configuration.calendar : null);
            transactions.put(database, transaction);
        }
        return transaction;
    }

    /**
     * Commits the thread local transaction for the given database name if it
     * has been opened.
     *
     * @param database the name of the database.
     * @return the closed transaction or null for no active transaction.
     * @throws SQLException if a database access error occur
     */
    public static Transaction commit(String database) throws SQLException {
        database = context(database).effectiveName();
        HashMap<String, Transaction> transactions = get().getTransactions();
        Transaction transaction = transactions.get(database);
        if (transaction != null) {
            transaction.commit();
        } else {
            assertConfigured(database);
        }
        return transaction;
    }

    /**
     * Closes the thread local transaction for the given database name if it has
     * been opened. This method is idempotent when called from the same thread.
     *
     * @param database the name of the database.
     * @return the closed transaction or null for no active transaction.
     */
    public static Transaction close(String database) {
        database = context(database).effectiveName();
        HashMap<String, Transaction> transactions = get().getTransactions();
        Transaction transaction = transactions.get(database);
        if (transaction != null) {
            transaction.close();
        } else {
            assertConfigured(database);
        }
        return transaction;
    }

    /**
     * Closes and destroys all transactions for the current thread.
     */
    public static void close() {
        HashMap<String, Transaction> map = get().getTransactions();
        for (Transaction transaction : map.values()) {
            transaction.destroy();
        }
        map.clear();
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
     * database.moria@development.dataSource.url=jdbc:postgresql://sjhdb05b.jajja.local:5432/moria_development
     * database.moria@development.dataSource.username=dev
     * database.moria@development.dataSource.password=$43CR37
     *
     * database.moria@production.dataSource.url=jdbc:postgresql://sjhdb05b.jajja.local:5432/moria_production
     * database.moria@production.dataSource.username=prod
     * database.moria@production.dataSource.password=$43CR37:P455
     *
     * database.context=
     * database.moria.context=production
     */
    private void configure() {
        try {
            Enumeration<URL> urls = Thread.currentThread().getContextClassLoader().getResources("jorm.properties");
            URL local = null;
            while (urls.hasMoreElements()) {
                URL url = urls.nextElement();
                if (url.getProtocol().equals("jar")) {
                    //Database.get().log.debug("Found jorm configuration @ " + url.toString());
                    configure(url);
                } else {
                    local = url;
                }
            }
            if (local != null) {
                //Database.get().log.debug("Found jorm configuration @ " + local.toString());
                configure(local);
            }

            for (Entry<String, Configuration> entry : configurations.entrySet()) {
                String database = entry.getKey();
                Configuration configuration = entry.getValue();
                int index = database.indexOf(Context.CONTEXT_SEPARATOR);
                if (index > 0) {
                    Configuration base = configurations.get(database.substring(0, index));
                    if (base != null) {
                        configuration.inherit(base);
                    }
                }
                configuration.init();
                configure(database, configuration.dataSource);
                //Database.get().log.debug("Configured " + configuration);
            }
        } catch (IOException ex) {
            //Database.get().log.warn("Failed to find resource 'jorm.properties': " + ex.getMessage(), ex);
        }
    }

    public static void configure(URL url) throws IOException {
        Properties properties = new Properties();
        InputStream is = url.openStream();
        properties.load(is);
        is.close();
        configure(properties);
    }

    public static void configure(Properties properties) {
        for (Entry<Object, Object> property : properties.entrySet()) {
            String[] parts = ((String)property.getKey()).split("\\.");
            boolean isMalformed = false;

            if (parts[0].equals("database") && parts.length > 1) {
                String database = parts[1];

                Configuration configuration = get().configurations.get(database);
                if (configuration == null) {
                    configuration = new Configuration(database);
                    get().configurations.put(database, configuration);
                }

                String value = (String)property.getValue();
                switch (parts.length) {
                case 2:
                    if (parts[1].equals("context")) {
                        Database.get().globalDefaultContext = value;
                    } else {
                        isMalformed = true;
                    }
                    break;

                case 3:
                    if (parts[2].equals("destroyMethod")) {
                        configuration.destroyMethodName = value;
                    } else if (parts[2].equals("dataSource")) {
                        configuration.dataSourceClassName = value;
                    } else if (parts[2].equals("defaultContext")) {
                        if (database.indexOf(Context.CONTEXT_SEPARATOR) != -1) {
                            isMalformed = true;
                        } else {
                            Database.get().defaultContext.put(database, value);
                        }
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
                //Database.get().log.warn(String.format("Malformed jorm property: %s", property.toString()));
                throw new RuntimeException(String.format("Malformed jorm property: %s", property.toString()));
            }
        }
    }

    public static class Configuration {
        private String database;
        private String dataSourceClassName;
        private String destroyMethodName;
        private Map<String, String> dataSourceProperties = new HashMap<String, String>();
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
                                //get().log.warn("Failed to invoke " + dataSource.getClass().getName() + "#" + method.getName() + "() in configuration of '" + database + "'", e);
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
                    //get().log.error("Failed to invoke destroy method for " + dataSource.getClass(), e);
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

    public static class Context implements Closeable {
        public static final char CONTEXT_SEPARATOR = '@';
        private Context prev;
        private Context next;
        private String database;
        private String name;
        private boolean isClosed = false;

        // Root context
        private Context(String database) {
            this.database = database;
        }

        private Context(Context previous, String name) {
            this.database = previous.database;
            this.name = name;
            this.prev = previous;
            previous.next = this;
        }

        public String database() {
            return database;
        }

        public String name() {
            return name;
        }

        public String effectiveName() {
            String effectiveName = database;
            String name = this.name;
            if (name == null) {
                name = defaultContext(database);
            }
            if (name == null) {
                name = globalDefaultContext();
            }
            if (name == null) {
                name = "";
            }
            if (!name.isEmpty()) {
                effectiveName += CONTEXT_SEPARATOR + name;
            }
            return effectiveName;
        }

        @Override
        public void close() {
            if (prev == null) {
                throw new IllegalStateException("Attempt to close root context");
            }
            if (next != null || isClosed) {
                throw new IllegalStateException("Context closed in wrong order");
            }
            contextStack().put(database, prev);
            prev.next = null;
            prev = null;
            isClosed = true;
        }

        @Override
        public String toString() {
            return String.format("%s { database=%s, name=%s, prev=%s, next=%s }", getClass(), database, name, prev != null ? "yes" : "no", next != null ? "yes" : "no");
        }
    }

    // Get default global context (not thread safe)
    public static String globalDefaultContext() {
        return get().globalDefaultContext;
    }

    // Set default global context (not thread safe)
    public static String globalDefaultContext(String name) {
        if (name == null) {
            name = "";
        }
        String previous = globalDefaultContext();
        get().globalDefaultContext = name;
        return previous;
    }

    // Get default per database context (not thread safe)
    public static String defaultContext(String database) {
        return get().defaultContext.get(database);
    }

    // Set default per database context (not thread safe)
    public static String defaultContext(String database, String name) {
        return get().defaultContext.put(database, name);
    }

    private static HashMap<String, Context> contextStack() {
        HashMap<String, Context> map = get().contextStack.get();
        if (map == null) {
            map = new HashMap<String, Context>();
            get().contextStack.set(map);
        }
        return map;
    }

    // Get active thread-local context
    public static Context context(String database) {
        HashMap<String, Context> map = contextStack();
        Context activeContext = map.get(database);
        if (activeContext == null) {
            activeContext = new Context(database);
            map.put(database, activeContext);
        }
        return activeContext;
    }

    // Push active thread-local context
    public static Context context(String database, String name) {
        HashMap<String, Context> map = contextStack();
        Context newContext = new Context(context(database), name);
        map.put(database, newContext);
        return newContext;
    }
}
