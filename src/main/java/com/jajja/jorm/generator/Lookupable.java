package com.jajja.jorm.generator;

import java.util.List;

public interface Lookupable {
    public Lookupable lookup(List<String> path, Class<? extends Lookupable> expectedClass);
}
