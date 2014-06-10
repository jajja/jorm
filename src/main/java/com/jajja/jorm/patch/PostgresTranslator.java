package com.jajja.jorm.patch;

import com.jajja.jorm.patch.postgres.PGobject;

public class PostgresTranslator extends Translator {

    @Override
    public Object translate(Object o) {
        try {
            if (o instanceof org.postgresql.util.PGobject && !(o instanceof PGobject)) {
                return new PGobject((org.postgresql.util.PGobject) o);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to translate PGObject", e);
        }
        return o;
    }


}
