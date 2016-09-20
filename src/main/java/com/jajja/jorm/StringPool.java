package com.jajja.jorm;

import java.lang.ref.WeakReference;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.WeakHashMap;

public class StringPool {
    private static WeakHashMap<String, WeakReference<String>> stringMap = new WeakHashMap<String, WeakReference<String>>();

    public static String get(String str) {
        synchronized (stringMap) {
            WeakReference<String> stringRef = stringMap.get(str);
            if (stringRef != null) {
                String existingString = stringRef.get();
                if (existingString != null) {
                    return existingString;
                }
            }
            stringMap.put(str, new WeakReference<String>(str));
            return str;
        }
    }

    public static String[] array(String ... strings) {
        String[] ret = new String[strings.length];
        synchronized (stringMap) {
            for (int i = 0; i < strings.length; i++) {
                String str = strings[i];
                WeakReference<String> stringRef = stringMap.get(str);
                if (stringRef != null) {
                    ret[i] = stringRef.get();
                    if (ret[i] != null) {
                        continue;
                    }
                }
                ret[i] = str;
                stringMap.put(str, new WeakReference<String>(str));
            }
        }
        return ret;
    }

    public static String[] arrayFromColumnLabels(ResultSetMetaData metaData) throws SQLException {
        String[] ret = new String[metaData.getColumnCount()];
        synchronized (stringMap) {
            for (int i = 0; i < ret.length; i++) {
                String str = metaData.getColumnLabel(i + 1);
                WeakReference<String> stringRef = stringMap.get(str);
                if (stringRef != null) {
                    ret[i] = stringRef.get();
                    if (ret[i] != null) {
                        continue;
                    }
                }
                ret[i] = str;
                stringMap.put(str, new WeakReference<String>(str));
            }
        }
        return ret;
    }
}
