package com.jajja.jorm.patch;

import java.util.HashMap;

public abstract class Translator {

    private static final HashMap<Class<?>, Translator> TRANSLATORS = new HashMap<Class<?>, Translator>();

    private static final Translator DEFAULT_TRANSLATOR = new DefaultTranslator();

    static {
        try {
            Class<?> pgo = Class.forName("org.postgresql.util.PGobject");
            TRANSLATORS.put(pgo, new PostgresTranslator());
        } catch (ClassNotFoundException e) {
        }
    }

    public static Translator get(Class<?> clazz) {
        if (TRANSLATORS.isEmpty()) {
            return DEFAULT_TRANSLATOR;
        } else {
            Translator translator = TRANSLATORS.get(clazz);
            return translator != null ? translator : Translator.DEFAULT_TRANSLATOR;
        }
    }

    private static class DefaultTranslator extends Translator {
        @Override
        public Object translate(Object o) {
            return o;
        }
    }

    public abstract Object translate(Object o);

}
