package com.jajja.jorm.patch;

import org.postgresql.util.PGobject;

public class PostgresTranslator extends Translator {

    @Override
    public Object translate(Object o) {
        try {
            if (o instanceof PGobject && !(o instanceof FixedPGobject)) {
                PGobject pgObject = (PGobject) o;
                return new FixedPGobject(pgObject.getType(), pgObject.getValue());
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to translate PGObject", e);
        }
        return o;
    }


}
