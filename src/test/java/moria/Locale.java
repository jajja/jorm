package moria;

import com.jajja.jorm.Jorm;
import com.jajja.jorm.Record;

@Jorm(table="locales", primaryKey={"language", "country"})
public class Locale extends Record {
    public Integer getId() {
        return get("id", Integer.class);
    }
    public void setId(Integer id) {
        set("id", id);
    }
    public String getLanguage() {
        return get("language", String.class);
    }
    public void setLanguage(String language) {
        set("language", language);
    }
    public String getCountry() {
        return get("country", String.class);
    }
    public void setCountry(String country) {
        set("country", country);
    }
    public String getName() {
        return get("name", String.class);
    }
    public void setName(String name) {
        set("name", name);
    }
}
