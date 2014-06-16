package com.jajja.jorm.patch.postgres;

import java.sql.SQLException;

@SuppressWarnings("serial")
public class FixedPGobject extends org.postgresql.util.PGobject {
    public FixedPGobject() {
        super();
    }

    public FixedPGobject(org.postgresql.util.PGobject o) {
        super();
        setType(o.getType());
        try {
            setValue(o.getValue());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int hashCode() {
        return getType().hashCode() + getValue().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FixedPGobject) {
            return ((FixedPGobject)obj).getValue().equals(getValue()) && ((FixedPGobject)obj).getType().equals(getType());
        }
        return false;
    }
}
