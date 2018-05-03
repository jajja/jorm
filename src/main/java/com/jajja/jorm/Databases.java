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

/**
 * Database configuration meta class, maintaining active database configurations.
 *
 * @see Database
 * @see Database
 * @author Martin Korinth &lt;martin.korinth@jajja.com&gt;
 * @since 2.0.0
 */
public class Databases {
    private final Logger logger = Logger.getLogger(Databases.class.getName());
    private final ConcurrentHashMap<String, Database> databases = new ConcurrentHashMap<String, Database>(16, 0.75f, 1);

    public Database get(String databaseName) {
        return databases.get(databaseName);
    }

    public Database put(Database database) {
        if (database == null) {
            throw new NullPointerException("database is null");
        }
        return databases.put(database.getName(), database);
    }

    public void remove(Database database) {
        if (database == null) {
            throw new NullPointerException("database is null");
        }
        remove(database.getName());
    }

    public void remove(String databaseName) {
        databases.remove(databaseName);
    }

    public void destroy() {
        for (Database database : databases.values()) {
            database.destroy();
        }
    }

    public void configure() {
        try {
            Enumeration<URL> urls = Thread.currentThread().getContextClassLoader().getResources("jorm.properties");
            List<URL> locals = new LinkedList<URL>();
            while (urls.hasMoreElements()) {
                URL url = urls.nextElement();
                if (url.getProtocol().equals("jar")) {
                    logger.log(Level.FINE, "Found jorm configuration @ " + url.toString());
                    configureFrom(url);
                } else {
                    locals.add(url);
                }
            }
            for (URL url : locals) {
                logger.log(Level.FINE, "Found jorm configuration @ " + url.toString());
                configureFrom(url);
            }
        } catch (IOException ex) {
            throw new RuntimeException("Failed to configure from jorm.properties", ex);
        }
    }

    public void configureFrom(Properties properties) {
        configureFrom(properties, false);
    }

    public void configureFrom(Properties properties, boolean merge) {
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
        for (Entry<String, Properties> e : confs.entrySet()) {
            String databaseName = e.getKey();
            Properties props = e.getValue();

            put(new Database.PropertyConfiguredDatabase(databaseName, props));
        }
    }

    public void configureFrom(URL url) throws IOException {
        InputStream is = url.openStream();
        try {
            Properties properties = new Properties();
            properties.load(is);
            configureFrom(properties);
        } finally {
            is.close();
        }
    }
}
