package psql;

import java.sql.SQLException;
import java.util.List;

import junit.framework.Assert;

import moria.Goblin;
import moria.Litter;
import moria.Tribe;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.jajja.jorm.Column;
import com.jajja.jorm.Database;
import com.jajja.jorm.Record;
import com.jajja.jorm.exceptions.CheckViolationException;
import com.jajja.jorm.exceptions.UniqueViolationException;

public class Moria {

    @BeforeClass
    public static void open() {
        DataSource dataSource = new DataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl("jdbc:postgresql://sjhdb05b.jajja.local:5432/moria");
        dataSource.setUsername("gandalf");
        dataSource.setPassword("mellon");
        Database.configure("moria", dataSource);
        try {
            Database.open("moria").load(ClassLoader.class.getResourceAsStream("/moria.sql"));
            Database.commit("moria");
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
            Goblin goblin = Record.find(Goblin.class);
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
            Goblin goblin = Record.find(Goblin.class, new Column("name", "Bolg"));
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
            Goblin goblin = Record.find(Goblin.class, new Column("name", "Bolg"));
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
            Tribe tribe = Record.find(Tribe.class);
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
            Litter litter = Record.find(Litter.class);
            litter.setStench(2.);
            litter.save();
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
        } catch (SQLException e2) {
            e = e2;
            Database.close("moria");
        }
        Assert.assertTrue(e instanceof UniqueViolationException);
    }

}
