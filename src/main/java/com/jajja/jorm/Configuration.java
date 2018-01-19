package com.jajja.jorm;

import java.lang.reflect.Method;
import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;

import javax.sql.DataSource;

public abstract class Configuration {

    public static Configuration get(DataSource dataSource) {
        return new DataSourceConfiguration(dataSource);
    }

    public static Configuration get(Properties properties) {
        return new PropertiesConfiguration(properties);
    }

    public abstract DataSource getDataSource();

    public abstract Calendar getCalendar();

    public abstract void destroy();

    private static class DataSourceConfiguration extends Configuration {
        private DataSource dataSource;
        private DataSourceConfiguration(DataSource dataSource) {
            this.dataSource = dataSource;
        }
        @Override
        public DataSource getDataSource() {
            DataSource value = dataSource;
            if (value == null) {
                throw new RuntimeException("Configuration destroyed!");
            }
            return value;
        }
        @Override
        public Calendar getCalendar() {
            return null;
        }
        @Override
        public void destroy() {
            dataSource = null;
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
    private static class PropertiesConfiguration extends Configuration {

        private DataSource dataSource;
        private Method destroy;
        private Calendar calendar;

        public PropertiesConfiguration(Properties properties) {
            try {
                Class<?> type = Class.forName(properties.getProperty("dataSource"));
                if (properties.getProperty("destroyMethod") != null) {
                    try {
                        destroy = type.getMethod(properties.getProperty("destroyMethod"));
                    } catch (NoSuchMethodException e) {
                        throw new IllegalArgumentException("The destroy method does not exist!", e);
                    } catch (SecurityException e) {
                        throw new IllegalArgumentException("The destroy method is not accessible!", e);
                    }
                }
                String timeZone = properties.getProperty("timeZone", "default");
                if ("default".equalsIgnoreCase(timeZone)) {
                    calendar = null;
                } else {
                    calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone));
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
                throw new IllegalArgumentException("The data source implementation " + properties.getProperty("dataSource") + " has no default constructor!", e);
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException("The data source implementation " + properties.getProperty("dataSource") + " has no public constructor!", e);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("The data source implementation " + properties.getProperty("dataSource") + " does not exist!", e);
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("The data source implementation " + properties.getProperty("dataSource") + " is not a data source!", e);
            }
        }

        @Override
        public DataSource getDataSource() {
            return dataSource;
        }

        @Override
        public Calendar getCalendar() {
            return calendar;
        }

        @Override
        public void destroy() {
            if (destroy != null) {
                try {
                    destroy.invoke(dataSource);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to invoke destroy method: " + dataSource.getClass().getName() + "#" + destroy.getName() + "()", e);
                }
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
        } else { // XXX: complex objects?
            object = null;
        }
        return (T) object;
    }

}
