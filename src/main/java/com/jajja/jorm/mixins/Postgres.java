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
package com.jajja.jorm.mixins;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.joda.time.Period;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGobject;

import com.jajja.jorm.Row;
import com.jajja.jorm.Transaction;

/**
 * <p>
 * Experimental Postgres functionality extensions that can be mixed in with
 * static imports.
 * </p>
 * <p>
 * <strong>Note:</strong> experimental, may dissapear or change
 * drastically in upcoming versions of jORM.
 * </p>
 *
 * @author Andreas Allerdahl &lt;andreas.allerdahl@jajja.com&gt;
 * @since 1.0.0
 */
public final class Postgres {
    @Deprecated
    public static <T> List<T> fromArray(Class<T> clazz, Array array) throws SQLException {
        return toList(clazz, array);
    }

    public static <T> List<T> toList(Class<T> clazz, Array array) throws SQLException {
        List<T> values = null;
        if (array != null) {
            values = new LinkedList<T>();
            ResultSet rs = array.getResultSet();
            while (rs.next()) {
                values.add(Row.convert(rs.getObject(2), clazz));
            }
        }
        return values;
    }

    public static Array toArray(Transaction transaction, String sqlDataType, Collection<? extends Object> values) throws SQLException {
        return values != null ? transaction.getConnection().createArrayOf(sqlDataType, values.toArray()) : null;
    }

    public static Period toPeriod(PGInterval interval) {
        if (interval == null) {
            return null;
        }

        int seconds = (int)interval.getSeconds();
        int millis = (int)(interval.getSeconds() * 1000.0 - seconds);

        return new Period()
            .plusYears(interval.getYears())
            .plusMonths(interval.getMonths())
            .plusDays(interval.getDays())
            .plusHours(interval.getHours())
            .plusMinutes(interval.getMinutes())
            .plusSeconds(seconds)
            .plusMillis(millis);
    }

    public static PGInterval toInterval(Period period) {
        if (period == null) {
            return null;
        }
        return new PGInterval(
                    period.getYears(),
                    period.getMonths(),
                    period.getDays(),
                    period.getHours(),
                    period.getMinutes(),
                    period.getSeconds() + (double)period.getMillis() / 1000
                );
    }

    public static PGobject pgObject(String type, String value) {
        if (value == null) {
            return null;
        }

        PGobject obj = new PGobject();
        obj.setType(type);
        try {
            obj.setValue(value);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return obj;
    }

    @Deprecated
    public static PGobject toEnum(String type, String value) {
        return pgObject(type, value);
    }

    public static String pgObjectValue(PGobject o, String type) {
        if (o == null) {
            return null;
        }
        if (type != null && !o.getType().equals(type)) {
            throw new IllegalStateException("expected type " + type + ", found " + o.getType());
        }
        return o.getValue();
    }

    public static String pgObjectValue(PGobject o) {
        return pgObjectValue(o, null);
    }

    @Deprecated
    public static String get(PGobject o, String type) {
        return pgObjectValue(o, type);
    }

    public static InetAddress toInetAddress(PGobject o) {
        String address = pgObjectValue(o, "inet");
        if (address == null) {
            return null;
        }
        try {
            return InetAddress.getByName(address);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public static PGobject toInet(InetAddress inetAddress) {
        if (inetAddress == null) {
            return null;
        }
        return pgObject("inet", inetAddress.getHostAddress());
    }
}
