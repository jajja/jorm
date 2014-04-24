package psql;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import junit.framework.Assert;
import moria.Goblin;
import moria.Litter;
import moria.Locale;
import moria.Tribe;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.jajja.jorm.Database;
import com.jajja.jorm.Record;
import com.jajja.jorm.exceptions.CheckViolationException;
import com.jajja.jorm.exceptions.UniqueViolationException;

public class Moria {

    @BeforeClass
    public static void open() {
        try {
            Database.open("moria").load(ClassLoader.class.getResourceAsStream("/moria.sql"));
            Database.commit("moria");
            Database.open("moria").setLoggingEnabled(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void close() {
        Database.close("moria");
    }


    @Test
    public void t01_find() {
        try {
            Goblin goblin = Record.select(Goblin.class, "SELECT * FROM #1# LIMIT 1", Goblin.class);
            Assert.assertNotNull(goblin);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
            Database.close("moria");
        }
    }

    @Test
    public void t02_findAll() {
        try {
            List<Goblin> goblins = Record.findAll(Goblin.class);
            Assert.assertFalse(goblins.isEmpty());
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
            Database.close("moria");
        }
    }

    @Test
    public void t03_columns() {
        try {
            Goblin goblin = Record.select(Goblin.class, "SELECT * FROM #1# WHERE name = 'Bolg'", Goblin.class);
            Assert.assertNotNull(goblin);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
            Database.close("moria");
        }
    }

    @Test
    public void t04_oneToOne() {
        try {
            Goblin goblin = Record.select(Goblin.class, "SELECT * FROM #1# WHERE name = 'Bolg'", Goblin.class);
            Tribe tribe = goblin.getTribe();
            Assert.assertNotNull(tribe);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
            Database.close("moria");
        }
    }

    @Test
    public void t05_oneToMany() {
        try {
            Tribe tribe = Record.select(Tribe.class, "SELECT * FROM #1# LIMIT 1", Tribe.class);
            List<Goblin> goblins = tribe.getGoblins();
            Assert.assertFalse(goblins.isEmpty());
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
            Database.close("moria");
        }
    }

    @Test
    public void t06_queryField() {
        try {
            List<Goblin> goblins = Record.findAll(Goblin.class);
            for (Goblin goblin : goblins) {
                Litter litter = goblin.relieve();
                litter.save();
            }
            Assert.assertEquals(Record.findAll(Litter.class).size(), goblins.size());
            Database.commit("moria");
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
            Database.close("moria");
        }
    }

    @Test
    public void t07_checkViolation() {
        SQLException e = null;
        try {
            Litter litter = Record.select(Litter.class, "SELECT * FROM #1# LIMIT 1", Litter.class);
            litter.setStench(2.);
            litter.save();
            Database.commit("moria");
        } catch (SQLException e2) {
            e = e2;
            Database.close("moria");
        }
        Assert.assertTrue(e instanceof CheckViolationException);
    }

    @Test
    public void t08_uniqueViolation() {
        SQLException e = null;
        try {
            Goblin goblin = new Goblin();
            goblin.setName("Bolg");
            goblin.setTribeId(1);
            goblin.insert();
            Database.commit("moria");
        } catch (SQLException e2) {
            e = e2;
            Database.close("moria");
        }
        Assert.assertTrue(e instanceof UniqueViolationException);
    }

    @Test
    public void t09_compositeKey() {
        try {
            Locale locale = Locale.findById(Locale.class, Record.primaryKey(Locale.class).value("sv", "SE"));
            locale.setName("Swedish");
            locale.save();
            locale = new Locale();
            locale.setLanguage("de");
            locale.setCountry("DE");
            locale.save(Record.ResultMode.NO_RESULT);
        } catch (SQLException e) {
        }
        Database.close("moria");
    }

    @Test
    public void t10_environment() {
        Database.set("test");
        try {
            Database.open("moria").load(ClassLoader.class.getResourceAsStream("/moria.sql")); // per transaction!
            Goblin goblin = Record.select(Goblin.class, "SELECT * FROM #1# LIMIT 1", Goblin.class);
            Assert.assertNotNull(goblin);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
            Database.close("moria");
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Database.unset();
    }
}
