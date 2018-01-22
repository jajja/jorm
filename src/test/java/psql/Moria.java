package psql;

import java.sql.SQLException;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jajja.jorm.Configurations;
import com.jajja.jorm.Database;
import com.jajja.jorm.Record;
import com.jajja.jorm.Transaction;
import com.jajja.jorm.exceptions.CheckViolationException;
import com.jajja.jorm.exceptions.UniqueViolationException;

import moria.Goblin;
import moria.Litter;
import moria.Locale;
import moria.Tribe;

public class Moria {
    Logger log = LoggerFactory.getLogger(Moria.class);

    static Database database;

    @BeforeClass
    public static void open() {
        Configurations.load();
        database = new Database("moria");
        Transaction t = database.open();
        try {
            t.load(ClassLoader.class.getResourceAsStream("/moria.sql"));
            t.commit();
            t.addListener(new Transaction.StdoutLogListener());
        } catch (Exception e) {
            LoggerFactory.getLogger(Moria.class).error("Failed to open test", e);
        } finally {
            t.close();
        }
    }

    @AfterClass
    public static void close() {
        database.close();
        Configurations.destroy();
    }

    @Test
    public void t01_find() {
        Transaction t = database.open();
        try {
            Goblin goblin = t.select(Goblin.class, "SELECT * FROM #1# LIMIT 1", Goblin.class);
            Assert.assertNotNull(goblin);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            t.close();
        }
    }

    @Test
    public void t02_findAll() {
        Transaction t = database.open();
        try {
            List<Goblin> goblins = t.findAll(Goblin.class);
            Assert.assertFalse(goblins.isEmpty());
        } catch (SQLException e) {
            log.error("Fail caused by SQL exception", e);
            Assert.fail();
        } finally {
            t.close();
        }
    }

    @Test
    public void t03_columns() {
        Transaction t = database.open();
        try {
            Goblin goblin = t.select(Goblin.class, "SELECT * FROM #1# WHERE name = 'Bolg'", Goblin.class);
            Assert.assertNotNull(goblin);
        } catch (SQLException e) {
            log.error("Fail caused by SQL exception", e);
            Assert.fail();
        } finally {
            t.close();
        }
    }

    @Test
    public void t04_oneToOne() {
        Transaction t = database.open();
        try {
            Goblin goblin = t.select(Goblin.class, "SELECT * FROM #1# WHERE name = 'Bolg'", Goblin.class);
            Tribe tribe = goblin.getTribe();
            Assert.assertNotNull(tribe);
        } catch (SQLException e) {
            log.error("Fail caused by SQL exception", e);
            Assert.fail();
        } finally {
            t.close();
        }
    }

    @Test
    public void t05_oneToMany() {
        Transaction t = database.open();
        try {
            Tribe tribe = t.select(Tribe.class, "SELECT * FROM #1# LIMIT 1", Tribe.class);
            List<Goblin> goblins = tribe.getGoblins(t);
            Assert.assertFalse(goblins.isEmpty());
        } catch (SQLException e) {
            log.error("Fail caused by SQL exception", e);
            Assert.fail();
        } finally {
            t.close();
        }
    }

    @Test
    public void t06_queryField() {
        Transaction t = database.open();
        try {
            List<Goblin> goblins = t.findAll(Goblin.class);
            System.out.println(goblins);
            for (Goblin goblin : goblins) {
                Litter litter = goblin.relieve(t);
                t.save(litter);
            }
            Assert.assertEquals(t.findAll(Litter.class).size(), goblins.size());
            t.commit();
        } catch (SQLException e) {
            log.error("Fail caused by SQL exception", e);
            Assert.fail();
        } finally {
            t.close();
        }
    }

    @Test
    public void t07_checkViolation() {
        Transaction t = database.open();
        SQLException e = null;
        try {
            Litter litter = t.select(Litter.class, "SELECT * FROM #1# LIMIT 1", Litter.class);
            litter.setStench(2.);
            t.save(litter);
        } catch (SQLException e2) {
            e = e2;
        } finally {
            t.close();
        }
        Assert.assertTrue(e instanceof CheckViolationException);
    }

    @Test
    public void t08_uniqueViolation() {
        SQLException e = null;
        Transaction t = database.open();
        try {
            Goblin goblin = new Goblin();
            goblin.setName("Bolg");
            goblin.setTribeId(1);
            t.insert(goblin);
            t.commit();
        } catch (SQLException e2) {
            e = e2;
        } finally {
            t.close();
        }
        Assert.assertTrue(e instanceof UniqueViolationException);
    }

    @Test
    public void t09_compositeKey() {
        Transaction t = database.open();
        try {
            Locale locale = t.findById(Locale.class, Record.primaryKey(Locale.class).value("sv", "SE"));
            locale.setName("Swedish");
            t.save(locale);
            locale = new Locale();
            locale.setLanguage("de");
            locale.setCountry("DE");
            t.save(locale, Record.ResultMode.NO_RESULT);
        } catch (SQLException e) {
            log.error("Fail caused by SQL exception", e);
            Assert.fail();
        } finally {
            t.close();
        }
    }
}
