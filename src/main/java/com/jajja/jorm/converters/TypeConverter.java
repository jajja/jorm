package com.jajja.jorm.converters;

public interface TypeConverter<T> {
    public boolean supports(Object nonNullValue);
    public T convert(Object value);
}
