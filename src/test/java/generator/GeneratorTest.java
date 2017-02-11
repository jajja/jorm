package generator;

import java.io.IOException;
import java.sql.SQLException;

import com.jajja.jorm.Database;
import com.jajja.jorm.generator.Generator;

public class GeneratorTest {

    public static void main(String[] args) throws IOException, SQLException {
        Database.open("moria").load(ClassLoader.class.getResourceAsStream("/moria.sql"));

        Generator generator = new Generator();

        generator.addDatabase("moria", "org.goblins.test.records")
            //.getDefaultSchema()
            .addTables("tribes", "goblins", "litters");

        generator.fetchMetadata();

        generator.getColumn("moria.@.goblins.tribe_id").addReference("tribes.id");
        generator.getColumn("moria.@.litters.goblin_id").addReference("goblins.id");

        generator.getTable("moria.@.tribes").addUnqiue("name");
        generator.getTable("moria.@.goblins").addUnqiue("tribe_id", "name");

        generator.writeFiles("/tmp/jorm-generator-test");

        System.out.println(generator);

        //com.jajja.jorm.Database.close("moria");
    }
}
