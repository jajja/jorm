package generator;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.dbcp.BasicDataSource;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.jajja.jorm.Database;
import com.jajja.jorm.Generator;

public class GeneratorTest {

	@BeforeClass
	public static void open() {
		BasicDataSource dataSource = new BasicDataSource();
		dataSource.setDriverClassName("org.postgresql.Driver");
		dataSource.setUrl("jdbc:postgresql://sjhdb05b.jajja.local:5432/moria");
		dataSource.setUsername("gandalf");
		dataSource.setPassword("mellon");        
		Database.configure("moria", dataSource);
		Database.open("moria").load(ClassLoader.class.getResourceAsStream("/moria.sql"));
	}
	
	@AfterClass
    public static void close() {
        Database.close("moria");
    }
	
	@Test
	public static void test() {
		try {
			Generator generator = new Generator("moria").file("src/test/java");
			generator.write("goblins");
			generator.write("litters");
			generator.write("tribes");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			Assert.fail();
		} catch (SQLException e) {
			e.printStackTrace();
			Assert.fail();
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	public static void main(String[] args) {
		open();
		test();
		close();
	}
	
}
