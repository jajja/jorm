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
import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;

/**
 * Configuration meta class, maintaining active database configurations.
 *
 * @see Database
 * @see Configuration
 * @author Martin Korinth &lt;martin.korinth@jajja.com&gt;
 * @since 2.0.0
 */
public class Configurations {

    private static final Logger logger = Logger.getLogger(Configurations.class.getName());
    private static final Map<String, Configuration> configurations = new ConcurrentHashMap<String, Configuration>(16, 0.75f, 1);

    public static void load() {
        try {
            Enumeration<URL> urls = Thread.currentThread().getContextClassLoader().getResources("jorm.properties");
            List<URL> locals = new LinkedList<URL>();
            while (urls.hasMoreElements()) {
                URL url = urls.nextElement();
                if (url.getProtocol().equals("jar")) {
                    logger.log(Level.FINE, "Found jorm configuration @ " + url.toString());
                    load(url);
                } else {
                    locals.add(url);
                }
            }
            for (URL url : locals) {
                logger.log(Level.FINE, "Found jorm configuration @ " + url.toString());
                load(url, true);
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
        configurations.put(database, configuration);
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

    public static void load(Properties properties) {
        load(properties, false);
    }

    public static void load(Properties properties, boolean isOverride) {
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

    public static void load(URL url) throws IOException {
        load(url, false);
    }

    public static void load(URL url, boolean isOverride) throws IOException {
        Properties properties = new Properties();
        InputStream is = url.openStream();
        properties.load(is);
        is.close();
        load(properties, isOverride);
    }


    public static Configuration getConfiguration(String database) {
        return configurations.get(database);
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

    public static void destroy() {
        if (configurations != null) {
            for (Configuration configuration : configurations.values()) {
                configuration.destroy();
            }
        }
    }

}
