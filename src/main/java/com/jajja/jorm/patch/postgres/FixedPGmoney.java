package com.jajja.jorm.patch.postgres;

import java.sql.SQLException;

@SuppressWarnings("serial")
public class FixedPGmoney extends org.postgresql.util.PGmoney {
    public FixedPGmoney() {
        super();
    }

    public FixedPGmoney(org.postgresql.util.PGmoney o) {
        super();
        setType(o.getType());
        try {
            setValue(o.getValue());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        val = o.val;
    }

    @Override
    public int hashCode() {
        return getType().hashCode() + getValue().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FixedPGmoney) {
            return ((FixedPGmoney)obj).getValue().equals(getValue()) && ((FixedPGmoney)obj).getType().equals(getType());
        }
        return false;
    }
}
