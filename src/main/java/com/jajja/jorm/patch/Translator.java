package com.jajja.jorm.patch;

import java.util.HashMap;

public abstract class Translator {

    static final HashMap<Class<?>, Translator> translators = new HashMap<Class<?>, Translator>();

    static {
        try {
            Class<?> pgo = Class.forName("org.postgresql.util.PGobject");
            translators.put(pgo, new PostgresTranslator());
        } catch (ClassNotFoundException e) {
        }
    }

    public static Translator get(Class<?> clazz) {
        if (translators.isEmpty()) {
            return null;
        } else {
            return translators.get(clazz);
        }
    }

    abstract Object translate(Object o);

}
