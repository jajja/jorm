package com.jajja.jorm.converters;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

public abstract class NumberConverter<T> implements TypeConverter<T> {
    protected static final BigDecimal floatMaxValue = new BigDecimal(Float.MAX_VALUE);
    protected static final BigDecimal doubleMaxValue = new BigDecimal(Double.MAX_VALUE);
    private static final Map<Class<? extends Number>, NumberConverter<?>> map = new HashMap<Class<? extends Number>, NumberConverter<?>>();

    static {
        map.put(Byte.class, new ByteConverter());
        map.put(Short.class, new ShortConverter());
        map.put(Integer.class, new IntegerConverter());
        map.put(Long.class, new LongConverter());
        map.put(Float.class, new FloatConverter());
        map.put(Double.class, new DoubleConverter());
        map.put(BigInteger.class, new BigIntegerConverter());
        map.put(BigDecimal.class, new BigDecimalConverter());
    }

    protected static void convertOverflow(Class<? extends Number> type, Number n) {
        throw new ArithmeticException(String.format("%s can not hold value %s", type.getSimpleName(), n.toString()));
    }

    @Override
    public boolean supports(Object nonNullValue) {
        return map.containsKey(nonNullValue.getClass());
    }

    public static class ByteConverter extends NumberConverter<Byte> {
        @Override
        public Byte convert(Object value) {
            final Number n = (Number)value;
            int v = n.intValue();
            if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
                convertOverflow(Byte.class, n);
            }
            return n.byteValue();
        }
    }

    public static class ShortConverter extends NumberConverter<Short> {
        @Override
        public Short convert(Object value) {
            final Number n = (Number)value;
            int v = n.intValue();
            if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
                convertOverflow(Short.class, n);
            }
            return n.shortValue();
        }
    }

    public static class IntegerConverter extends NumberConverter<Integer> {
        @Override
        public Integer convert(Object value) {
            final Number n = (Number)value;
            long v = n.longValue();
            if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
                convertOverflow(Integer.class, n);
            }
            return n.intValue();
        }
    }

    public static class LongConverter extends NumberConverter<Long> {
        @Override
        public Long convert(Object value) {
            final Number n = (Number)value;
            double v = n.doubleValue();
            if (v < Long.MIN_VALUE || v > Long.MAX_VALUE) {
                convertOverflow(Long.class, n);
            }
            return n.longValue();
        }
    }

    public static class FloatConverter extends NumberConverter<Float> {
        @Override
        public Float convert(Object value) {
            final Number n = (Number)value;
            if (n instanceof BigDecimal && ((BigDecimal)n).abs().compareTo(floatMaxValue) > 0) {
                convertOverflow(Float.class, n);
            } else if (n instanceof BigInteger && ((BigDecimal)n).abs().compareTo(floatMaxValue) > 0) {
                convertOverflow(Float.class, n);
            }
            double v = n.doubleValue();
            if (!Double.isInfinite(v) && (v < -Float.MAX_VALUE || v > Float.MAX_VALUE)) {
                convertOverflow(Float.class, n);
            }
            return n.floatValue();
        }
    }

    public static class DoubleConverter extends NumberConverter<Double> {
        @Override
        public Double convert(Object value) {
            final Number n = (Number)value;
            if (n instanceof BigDecimal && ((BigDecimal)n).abs().compareTo(doubleMaxValue) > 0) {
                convertOverflow(Double.class, n);
            } else if (n instanceof BigInteger && ((BigDecimal)n).abs().compareTo(doubleMaxValue) > 0) {
                convertOverflow(Double.class, n);
            }
            return n.doubleValue();
        }
    }

    public static class BigIntegerConverter extends NumberConverter<BigInteger> {
        @Override
        public BigInteger convert(Object value) {
            return new BigInteger(value.toString());
        }
    }

    public static class BigDecimalConverter extends NumberConverter<BigDecimal> {
        @Override
        public BigDecimal convert(Object value) {
            return new BigDecimal(value.toString());
        }
    }
}
