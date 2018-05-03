package moria;

import com.jajja.jorm.Jorm;
import com.jajja.jorm.Record;

@Jorm(table="litters", primaryKey="id", immutablePrefix="__")
public class Litter extends Record {
    public Integer getId() {
        return get("id", Integer.class);
    }
    public void setId(Integer id) {
        set("id", id);
    }
    public Integer getGoblinId() {
        return get("goblin_id", Integer.class);
    }
    public void setGoblinId(Integer goblinId) {
        set("goblin_id", goblinId);
    }
    public Goblin getGoblin() {
        return get("goblin_id", Goblin.class);
    }
    public void setGoblin(Goblin goblin) {
        set("goblin_id", goblin);
    }
    public Double getStench() {
        return get("stench", Double.class);
    }
    public void setStench(Double stench) {
        set("stench", stench);
    }
    public java.sql.Timestamp getLeftAt() {
        return get("__left_at", java.sql.Timestamp.class);
    }
}
