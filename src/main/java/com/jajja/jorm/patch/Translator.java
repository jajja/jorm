package com.jajja.jorm.patch;

import java.util.HashMap;

public abstract class Translator {

    static final HashMap<Class<?>, Translator> translators = new HashMap<Class<?>, Translator>();

    static final Translator translator = new DefaultTranslator();

    static {
        try {
            Class<?> pgo = Class.forName("org.postgresql.util.PGobject");
            translators.put(pgo, new PostgresTranslator());
        } catch (ClassNotFoundException e) {
        }
    }

    public static Translator get(Class<?> clazz) {
        if (translators.isEmpty()) {
            return translator;
        } else {
            Translator translator = translators.get(clazz);
            return translator != null ? translator : Translator.translator;
        }
    }

    private static class DefaultTranslator extends Translator {

        @Override
        Object translate(Object o) {
            return o;
        }

    }

    abstract Object translate(Object o);

}
