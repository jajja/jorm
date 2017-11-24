package moria;

import java.sql.SQLException;
import java.util.List;

import com.jajja.jorm.Jorm;
import com.jajja.jorm.Record;

@Jorm(database="moria", table="tribes", primaryKey="id")
public class Tribe extends Record {
    public Integer getId() {
        return get("id", Integer.class);
    }
    public void setId(Integer id) {
        set("id", id);
    }
    public String getName() {
        return get("name", String.class);
    }
    public void setName(String name) {
        set("name", name);
    }
    public List<Goblin> getGoblins() throws SQLException {
        return transaction().selectAll(Goblin.class, "SELECT * FROM #1# WHERE tribe_id = #2#", Goblin.class, getId());
    }
}
