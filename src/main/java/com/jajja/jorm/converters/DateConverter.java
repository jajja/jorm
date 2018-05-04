package com.jajja.jorm.converters;



public abstract class DateConverter<T> implements TypeConverter<T> {
//    protected static final BigDecimal floatMaxValue = new BigDecimal(Float.MAX_VALUE);
//    protected static final BigDecimal doubleMaxValue = new BigDecimal(Double.MAX_VALUE);
//    private static final Map<Class<? extends Number>, DateConverter<?>> map = new HashMap<Class<? extends Number>, DateConverter<?>>();
//    private static boolean haveJodaTime = false;
//    private static boolean haveJava8Time = false;
//
//    static {
//        map.put(Date.class, new ByteConverter());
//        map.put(Timestamp.class, new ShortConverter());
//        map.put(java.sql.Date.class, new IntegerConverter());
//        try {
//            map.put(org.joda.time.DateTime.class, new LongConverter());
//            map.put(org.joda.time.LocalDate.class, new BigIntegerConverter());
//            haveJodaTime = true;
//        } catch (ClassNotFoundException e) {
//        }
//        try {
//            map.put(java.time.OffsetDateTime.class, new BigDecimalConverter());
//            map.put(java.time.LocalDate.class, new BigDecimalConverter());
//            haveJava8Time = true;
//        } catch (ClassNotFoundException e) {
//        }
//    }
//
//    private static class NanoTime {
//        final long seconds;
//        final long nanos;
//        NanoTime(long seconds, long nanos) {
//            this.seconds = seconds;
//            this.nanos = nanos;
//        }
//        NanoTime(long millis) {
//            this.seconds = millis / 1000;
//            this.nanos = (millis % 1000) * 1000000L;
//        }
//    }
//
//    public static NanoTime toNanoTime(Object o) {
//        if (o == null) {
//            return null;
//        }
//        if (o instanceof Long) {
//            return new NanoTime((Long)o);
//        }
//        if (o instanceof java.util.Date) {
//            return new NanoTime( ((java.util.Date)o).getTime() );
//        }
//        if (o instanceof org.joda.time.ReadableInstant) {
//            return new NanoTime( ((org.joda.time.ReadableInstant)o).getMillis() );
//        }
//        if (o instanceof org.joda.time.LocalDate) {
//            return new NanoTime( ((org.joda.time.LocalDate)o).toDateTimeAtStartOfDay().getMillis() );
//        }
//        if (o instanceof java.time.OffsetDateTime) {
//            return new NanoTime( ((org.joda.time.ReadableInstant)o).getMillis() );
//        }
//        throw new IllegalArgumentException("Unknown date class " + o.getClass());
//    }
////
////    @Override
////    public boolean supports(Object nonNullValue) {
////        return map.containsKey(nonNullValue.getClass());
////    }
//
//    public static void main(String[] args) {
//        ZonedDateTime a = ZonedDateTime.now();
//        OffsetDateTime b = OffsetDateTime.now();
//        LocalDate c = LocalDate.now();
//        Date d = new Date(System.currentTimeMillis());
//        Timestamp e = new Timestamp(System.currentTimeMillis());
//        org.joda.time.DateTime f = new org.joda.time.DateTime(DateTimeZone.UTC);
//        org.joda.time.LocalDate g = new org.joda.time.LocalDate();
//
//        System.out.println(a);
//        System.out.println(b);
//        System.out.println(c);
//        System.out.println(d);
//        System.out.println(e);
//        System.out.println(f);
//        System.out.println(g);
//    }
}