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

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import org.joda.time.Period;
import org.postgresql.jdbc4.Jdbc4Array;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGobject;

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
 * @author Andreas Allerdahl <andreas.allerdahl@jajja.com>
 * @since 1.0.0
 */
public final class Postgres {
    public static List<String> stringArray(Jdbc4Array array) {
        List<String> values = new LinkedList<String>();

        if (array != null) {
            try {
                ResultSet rs = array.getResultSet();
                while (rs.next()) {
                    values.add(rs.getString(2));
                }
            } catch (SQLException e) {
                throw new RuntimeException("wtfbbq?");
            }
        }
        return values;
    }

    public static Array stringArray(Transaction transaction, Collection<String> values) {
        try {
            return transaction.getConnection().createArrayOf("varchar", values.toArray());
        } catch (SQLException e) {
            throw new RuntimeException("wtfbbq?");
        }
    }

    public static Period toPeriod(PGInterval interval) {
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
        return new PGInterval(
                period.getYears(),
                period.getMonths(),
                period.getDays(),
                period.getHours(),
                period.getMinutes(),
                period.getSeconds() + (double)period.getMillis() / 1000
            );
    }

    public static PGobject toEnum(String type, String value) {
        if (value == null) {
            return null;
        }

        PGobject obj = new PGobject();
        obj.setType(type);

        try {
            obj.setValue(value);
        } catch (SQLException e) {
            // UNREACHABLE
            throw new RuntimeException(e);
        }
        return obj;
    }
}
