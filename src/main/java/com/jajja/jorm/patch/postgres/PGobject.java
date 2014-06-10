package com.jajja.jorm.patch.postgres;

import java.sql.SQLException;


@SuppressWarnings("serial")
public class PGobject extends org.postgresql.util.PGobject {
    public PGobject() {
        super();
    }

    public PGobject(String type, String value) throws SQLException {
        super();
        setType(type);
        setValue(value);
    }

    public PGobject(org.postgresql.util.PGobject pgObject) throws SQLException {
        super();
        setType(pgObject.getType());
        setValue(pgObject.getValue());
    }

    @Override
    public int hashCode() {
        return getType().hashCode() + getValue().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PGobject) {
            return ((PGobject)obj).getValue().equals(getValue()) && ((PGobject)obj).getType().equals(getType());
        }
        return false;
    }
}
