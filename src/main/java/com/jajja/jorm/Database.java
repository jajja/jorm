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
import java.lang.reflect.Method;
import java.net.URL;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
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

    private Database() {
    }

    /**
     * Acts as singleton factory for bean configuration access. All other access
     * to databases should be static.
     *
     * @return the singleton database representation containing configured data
     * sources for databases.
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
        database = context.get(database);
        synchronized (dataSources) {
            return dataSources.get(database);
        }
    }

    /**
     * Configures all databases accessible through {@link Database#open(String)}
     * and {@link Database#close(String)}. Overrides any previous configuration.
     *
     * @param dataSources the named databases, each represented by a string and
     * a data source.
     */
    public void setDataSources(Map<String, DataSource> dataSources) {
        this.dataSources = dataSources;
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
        instance.dataSources.put(database, dataSource);
    }

    /**
     * Determines whether a named database has been configured or not.
     *
     * @param database the named database.
     * @return true if the named database has been configured, false otherwise.
     */
    public static boolean isConfigured(String database) {
        return instance.dataSources.containsKey(database);
    }

    /**
     * Ensures that a named database is configured by throwing an illegal state
     * exception if it is not.
     *
     * @param database the named database.
     * @throws IllegalStateException when the named database has not been
     * configured.
     */
    public static void ensureConfigured(String database) {
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
        database = context.get(database);
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

    public static Transaction open(String database, String context) {
        Database.context.setLocal(database, context);
        return open(database);
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
        database = context.get(database);
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
     * @param database the name of the database.
     * @return the closed transaction or null for no active transaction.
     */
    public static Transaction close(String database) {
        database = context.get(database);
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
        context.clear();
        HashMap<String, Transaction> map = instance.getTransactions();
        for (Transaction transaction : map.values()) {
            transaction.destroy();
        }
        map.clear();
        instance.transactions.remove();
    }
    private static Map<String, Configuration> configurations;

    private static final Context context = new Context();
    static {
        configure();
    }

    public static void set(String database, String context) {
        Database.context.setGlobal(database, context);
    }

    public static void set(String context) { // XXX: necessary?
        for (Configuration configuration : configurations.values()) {
            if (configuration.context != null) {
                set(configuration.database, context);
            }
        }
    }

    public String context(String database) {
        return context.get(database);
    }

    public static void destroy() {
        for (Configuration configuration : configurations.values()) {
            configuration.destroy();
        }
    }

    private static class Context {
        private static final char SEPARATOR = '@';
        private Map<String, String> global;
        private ThreadLocal<Map<String, String>> threadLocal;

        public Context() {
            global = new HashMap<String, String>();
            threadLocal = new ThreadLocal<Map<String, String>>();
        }

        private Map<String, String> local() {
            Map<String, String> map = threadLocal.get();
            if (map == null) {
                threadLocal.set(new HashMap<String, String>());
                map = threadLocal.get();
            }
            return map;
        }

        public void setGlobal(String database, String name) {
            global.put(database, name);
        }

        public void setLocal(String database, String name) {
            local().put(database, name);
        }

        public void clear() {
            local().clear();
        }

        public String get(String database) {
            if (0 < database.indexOf(SEPARATOR)) {
                return database;
            }
            String name = local().get(database);
            if (name == null) {
                name = global.get(database);
            }
            if (name.isEmpty()) {
                return database;
            } else {
                return database + SEPARATOR + name;
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
     * database.moria.context=production
     */
    private static void configure() {
        try {
            Enumeration<URL> urls = Thread.currentThread().getContextClassLoader().getResources("jorm.properties");
            configurations = new HashMap<String, Configuration>();
            URL local = null;
            while (urls.hasMoreElements()) {
                URL url = urls.nextElement();
                if (url.getProtocol().equals("jar")) {
                    configure(url);
                } else {
                    local = url;
                }
            }
            if (local != null) {
                configure(local);
            }
            Set<String> databases = new HashSet<String>();
            for (Entry<String, Configuration> entry : configurations.entrySet()) {
                String database = entry.getKey();
                int context = database.indexOf(Context.SEPARATOR);
                if (0 < context) {
                    database = database.substring(0, context);
                    Configuration base =  configurations.get(database);
                    if (base != null) {
                        entry.getValue().inherit(base);
                    }
                }
                databases.add(database);
                entry.getValue().apply();
                Database.get().log.debug("Configured " + entry.getValue());
            }
            for (Configuration configuration : configurations.values()) {
                if (configuration.context != null) {
                    context.setGlobal(configuration.database, configuration.context);
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
            Database.get().log.warn("Failed to find resource 'jorm.properties': " + ex.getMessage(), ex);
            configurations = null;
        }
    }

    private static void configure(URL url) {
        Database.get().log.debug("Found jorm configuration @ " + url.toString());

        Properties properties = new Properties();
        try {
            InputStream is = url.openStream();
            properties.load(is);
            is.close();
        } catch (IOException ex) {
            Database.get().log.error("Failed to open jorm.properties: " + ex.getMessage(), ex);
            return;
        }

        for (Entry<Object, Object> property : properties.entrySet()) {
            String[] parts = ((String) property.getKey()).split("\\.");
            if (parts.length < 3 || !parts[0].equals("database")) {
                continue;
            }
            String database = parts[1];
            Configuration configuration = configurations.get(database);
            if (configuration == null) {
                configurations.put(database, new Configuration(database));
                configuration = configurations.get(database);
            }
            String value = (String) property.getValue();
            switch (parts.length) {
            case 3:
                if (parts[2].equals("destroyMethod")) {
                    configuration.destroyMethodName = value;
                } else if (parts[2].equals("context")) {
                    configuration.context = value;
                } else if (parts[2].equals("dataSource")) {
                    configuration.dataSourceClassName = value;
                } else {
                    Database.get().log.warn(String.format("Unknown database attribute '%s' in jorm property: ‰s", parts[2], property.toString()));
                }
                break;
            case 4:
                if (parts[2].equals("dataSource")) {
                    configuration.dataSourceProperties.put(parts[3], value);
                } else {
                    Database.get().log.warn(String.format("Invalid jorm property: ‰s", property.toString()));
                }
                break;
            default:
                Database.get().log.warn(String.format("Invalid jorm property: ‰s", property.toString()));
            }
        }
    }

    public static class Configuration {
        private String database;
        private String context;
        private String dataSourceClassName;
        private String destroyMethodName;
        private Map<String, String> dataSourceProperties;
        private DataSource dataSource;
        private Method destroyMethod;

        @Override
        public String toString() {
            return "{ database => " + database + ", dataSourceClassName => " + dataSourceClassName + ", dataSourceProperties => " + dataSourceProperties + " }";
        }

        public void inherit(Configuration base) {
            if (dataSourceClassName == null) {
                dataSourceClassName = base.dataSourceClassName;
            }
            if (destroyMethodName == null) {
                destroyMethodName = base.destroyMethodName;
            }
            for (String key : base.dataSourceProperties.keySet()) {
                if (!dataSourceProperties.containsKey(key)) {
                    dataSourceProperties.put(key, base.dataSourceProperties.get(key));
                }
            }
            context = null;
        }

        public void apply() {
            init();
            configure(database, dataSource);
        }

//        public Configuration(String database, String dataSourceClassName, Map<String, String> dataSourceProperties, String destroyMethodName) {
//            this.database = database;
//            this.dataSourceClassName = dataSourceClassName;
//            this.destroyMethodName = destroyMethodName;
//            this.dataSourceProperties = dataSourceProperties;
//        }

        public Configuration(String database) {
            this.database = database;
            context = "";
            dataSourceProperties = new HashMap<String, String>();
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
                dataSource = (DataSource) type.newInstance();
                for (Method method : dataSource.getClass().getMethods()) {
                    String methodName = method.getName();
                    Class<?>[] parameterTypes = method.getParameterTypes();
                    if (methodName.startsWith("set") && 3 < methodName.length() && parameterTypes.length == 1) {
                        String name = method.getName().substring(3, 4).toLowerCase() + method.getName().substring(4); // setValue -> value
                        String property = dataSourceProperties.get(name);
                        if (property != null) {
                            boolean isAccessible = method.isAccessible();
                            method.setAccessible(true);
                            try {
                                method.invoke(dataSource, parse(method.getParameterTypes()[0], property));
                            } catch (Exception e) {
                                get().log.warn("Failed to invoke " + dataSource.getClass().getName() + "#" + method.getName() + "() in configuration of '" + database + "'", e);
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
                    get().log.error("Failed to invoke destroy method for " + dataSource.getClass(), e);
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
            return (T) object;
        }
    }
}
