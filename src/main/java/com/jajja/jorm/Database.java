package com.jajja.jorm;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;

import javax.sql.DataSource;

public class Database {
    protected final ThreadLocal<HashSet<Transaction>> transactions = new ThreadLocal<HashSet<Transaction>>();
    protected final String name;
    protected DataSource dataSource;
    protected Calendar defaultCalendar;

    protected Database(String name) {
        this.name = name;
    }

    public final String getName() {
        return name;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public Calendar getCalendar() {
        return defaultCalendar;
    }

    public void destroy() {
        dataSource = null;
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

    /**
     * Opens a transaction for the given database name.
     *
     * @param database the name of the database.
     * @return the open transaction.
     */
    public Transaction openTransaction() {
        return openTransaction(Transaction.class, null);
    }

    public Transaction openTransaction(Calendar calendar) {
        return openTransaction(Transaction.class, calendar);
    }

    public <T extends Transaction> T openTransaction(Class<T> clazz) {
        return openTransaction(clazz, null);
    }

    public <T extends Transaction> T openTransaction(Class<T> clazz, Calendar calendar) {
        try {
            Constructor<T> constructor = clazz.getDeclaredConstructor(Database.class, DataSource.class, Calendar.class);
            constructor.setAccessible(true);
            T t = constructor.newInstance(this, dataSource, calendar != null ? calendar : defaultCalendar);
            register(t);
            return t;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Set<Transaction> transactions() {
        return Collections.unmodifiableSet(getThreadLocalTransactions());
    }

    /**
     * Closes all transactions for the current thread.
     */
    public void close() {
        Set<Transaction> set = new HashSet<Transaction>(getThreadLocalTransactions());
        for (Transaction transaction : set) {
            transaction.close();
        }
        transactions.remove();
    }


    public static class DataSourceConfiguredDatabase extends Database {
        public DataSourceConfiguredDatabase(String name, DataSource dataSource, Calendar defaultCalendar) {
            super(name);
            this.dataSource = dataSource;
            this.defaultCalendar = defaultCalendar;
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
    public static class PropertyConfiguredDatabase extends Database {
        private Method destroyMethod;

        public PropertyConfiguredDatabase(String name, Properties properties) {
            super(name);
            process(properties);
        }

        private void process(Properties properties) {
            try {
                Class<?> type = Class.forName(properties.getProperty("dataSource"));
                if (properties.getProperty("destroyMethod") != null) {
                    try {
                        destroyMethod = type.getMethod(properties.getProperty("destroyMethod"));
                    } catch (NoSuchMethodException e) {
                        throw new IllegalArgumentException("The destroy method does not exist!", e);
                    } catch (SecurityException e) {
                        throw new IllegalArgumentException("The destroy method is not accessible!", e);
                    }
                }
                String timeZone = properties.getProperty("timeZone", "default");
                if ("default".equalsIgnoreCase(timeZone)) {
                    defaultCalendar = null;
                } else {
                    defaultCalendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone));
                }
                dataSource = (DataSource) type.newInstance();
                for (Method method : dataSource.getClass().getMethods()) {
                    String methodName = method.getName();
                    Class<?>[] parameterTypes = method.getParameterTypes();

                    if (methodName.startsWith("set") && methodName.length() > 3 && parameterTypes.length == 1) {
                        String name = method.getName().substring(3, 4).toLowerCase() + method.getName().substring(4); // setValue -> value
                        String property = properties.getProperty("dataSource." + name);
                        if (property != null) {
                            boolean isAccessible = method.isAccessible();
                            method.setAccessible(true);
                            try {
                                method.invoke(dataSource, parse(method.getParameterTypes()[0], property));
                            } catch (Exception e) {
                                throw new RuntimeException("Failed to invoke setter: " + dataSource.getClass().getName() + "#" + method.getName() + "()", e);
                            } finally {
                                method.setAccessible(isAccessible);
                            }
                        }
                    }
                }
            } catch (InstantiationException e) {
                throw new IllegalArgumentException("The data source implementation " + properties.getProperty("dataSource") + " has no default constructor", e);
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException("The data source implementation " + properties.getProperty("dataSource") + " has no public constructor", e);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("The data source implementation " + properties.getProperty("dataSource") + " does not exist", e);
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("The data source implementation " + properties.getProperty("dataSource") + " is not a data source", e);
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
            } else { // XXX: complex objects?
                object = null;
            }
            return (T) object;
        }

        @Override
        public void destroy() {
            if (destroyMethod != null) {
                try {
                    destroyMethod.invoke(dataSource);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to invoke destroy method: " + dataSource.getClass().getName() + "#" + destroyMethod.getName() + "()", e);
                }
            }
            super.destroy();
        }
    }
}
