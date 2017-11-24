package psql;

import java.sql.SQLException;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static Transaction open() {
        Transaction moria = Database.open("moria");
        try {
            Database.open("moria").load(ClassLoader.class.getResourceAsStream("/moria.sql"));
            Database.open("moria").addListener(new Transaction.StdoutLogListener());
        } catch (Exception e) {
            LoggerFactory.getLogger(Moria.class).error("Failed to open test", e);
        }
        return moria;
    }

    @AfterClass
    public static void close() {
        Database.close();
    }


    @Test
    public void t01_find() {
        Transaction moria = open();
        try {
            Goblin goblin = moria.select(Goblin.class, "SELECT * FROM #1# LIMIT 1", Goblin.class);
            Assert.assertNotNull(goblin);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
            moria.rollback();
        } finally {
            moria.close();
        }
    }

    @Test
    public void t02_findAll() {
        Transaction moria = open();
        try {
            List<Goblin> goblins = moria.findAll(Goblin.class);
            Assert.assertFalse(goblins.isEmpty());
        } catch (SQLException e) {
            log.error("Fail caused by SQL exception", e);
            Assert.fail();
            moria.rollback();
        } finally {
            moria.close();
        }
    }

    @Test
    public void t03_columns() {
        Transaction moria = open();
        try {
            Goblin goblin = moria.select(Goblin.class, "SELECT * FROM #1# WHERE name = 'Bolg'", Goblin.class);
            Assert.assertNotNull(goblin);
        } catch (SQLException e) {
            log.error("Fail caused by SQL exception", e);
            Assert.fail();
            moria.rollback();
        } finally {
            moria.close();
        }
    }

    @Test
    public void t04_oneToOne() {
        Transaction moria = open();
        try {
            Goblin goblin = moria.select(Goblin.class, "SELECT * FROM #1# WHERE name = 'Bolg'", Goblin.class);
            Tribe tribe = goblin.getTribe();
            Assert.assertNotNull(tribe);
        } catch (SQLException e) {
            log.error("Fail caused by SQL exception", e);
            Assert.fail();
            moria.rollback();
        } finally {
            moria.close();
        }
    }

    @Test
    public void t05_oneToMany() {
        Transaction moria = open();
        try {
            Tribe tribe = moria.select(Tribe.class, "SELECT * FROM #1# LIMIT 1", Tribe.class);
            List<Goblin> goblins = tribe.getGoblins();
            Assert.assertFalse(goblins.isEmpty());
        } catch (SQLException e) {
            log.error("Fail caused by SQL exception", e);
            Assert.fail();
            moria.rollback();
        } finally {
            moria.close();
        }
    }

    @Test
    public void t06_queryField() {
        Transaction moria = open();
        try {
            List<Goblin> goblins = moria.findAll(Goblin.class);
            for (Goblin goblin : goblins) {
                Litter litter = goblin.relieve();
                moria.save(litter);
            }
            Assert.assertEquals(moria.findAll(Litter.class).size(), goblins.size());
            moria.commit();
        } catch (SQLException e) {
            moria.rollback();
            log.error("Fail caused by SQL exception", e);
            Assert.fail();
        } finally {
            moria.close();
        }
    }

    @Test
    public void t07_checkViolation() {
        SQLException e = null;
        Transaction moria = open();
        try {
            Litter litter = Record.select(Litter.class, "SELECT * FROM #1# LIMIT 1", Litter.class);
            litter.setStench(2.);
            moria.save(litter);
            moria.commit();
        } catch (SQLException e2) {
            e = e2;
            moria.rollback();
        } finally {
            moria.close();
        }

        Assert.assertTrue(e instanceof CheckViolationException);
    }

    @Test
    public void t08_uniqueViolation() {
        SQLException e = null;
        Transaction moria = open();
        try {
            Goblin goblin = new Goblin();
            goblin.setName("Bolg");
            goblin.setTribeId(1);
            moria.insert(goblin);
            moria.commit();
        } catch (SQLException e2) {
            e = e2;
            moria.rollback();
        } finally {
            moria.close();
        }
        Assert.assertTrue(e instanceof UniqueViolationException);
    }

    @Test
    public void t09_compositeKey() {
        Transaction moria = open();
        try {
            Locale locale = moria.findById(Locale.class, Record.primaryKey(Locale.class).value("sv", "SE"));
            locale.setName("Swedish");
            moria.save(locale);
            locale = new Locale();
            locale.setLanguage("de");
            locale.setCountry("DE");
            moria.save(locale, Record.ResultMode.NO_RESULT);
        } catch (SQLException e) {
            log.error("Fail caused by SQL exception", e);
            Assert.fail();
            moria.rollback();
        } finally {
            moria.close();
        }
    }

}
