package moria;

import java.sql.SQLException;

import com.jajja.jorm.Composite;
import com.jajja.jorm.Jorm;
import com.jajja.jorm.Query;
import com.jajja.jorm.Record;
import com.jajja.jorm.Transaction;

@Jorm(database="moria", table="goblins", primaryKey="id")
public class Goblin extends Record {
    public Integer getId() {
        return get("id", Integer.class);
    }
    public void setId(Integer id) {
        set("id", id);
    }
    public Integer getTribeId() {
        return get("tribe_id", Integer.class);
    }
    public void setTribeId(Integer id) {
        set("tribe_id", id);
    }
    public Tribe getTribe() {
        return get("tribe_id", Tribe.class);
    }
    public void setTribe(Tribe tribe) {
        set("tribe_id", tribe);
    }
    public String getName() {
        return get("name", String.class);
    }
    public void setName(String name) {
        set("name", name);
    }
    public String getMindset() {
        return get("mindset", String.class);
    }
    public void setMindset(String mindset) {
        set("mindset", mindset);
    }

    // part 2
    public static Goblin findByTribeAndName(Transaction transaction, Tribe tribe, String name) throws SQLException {
        return transaction.find(Goblin.class, new Composite("tribe_id", "name").value(tribe, name));
    }

    // part 3
    public Litter relieve() throws SQLException {
        Litter litter = new Litter();
        litter.set("stench", new Query("random() * 0.9")) ;
        litter.setGoblin(this);
        return litter;
    }
}
