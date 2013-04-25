# jORM

jORM is a lightweight Java ORM. It does not aim at solving every database problem. It primarily cures the boilerplatisis that many Java solutions suffer from, while exposing the functionality of the JDBC through a convenient interface.

At [Jajja] [1] we've found that many applications have a need both for non teadious mapping of databases and for the full freedom of expression provided by raw SQL. This is an attempt to bridge the gap.

jORM has primarily been tested on Postgres, MySQL and MSSQL. Usage with other engines may lack some of the niftiness provided by the current dialect adoptions.


## Getting started

Would you like to be able to do the following without writing a single line of boilerplate?

    Goblin goblin = Record.findById(Goblin.class, 42);
    goblin.setName("Azog");
    goblin.save();
    goblin.commit();
    
You will be done before you are halfway through _getting started_.

### Getting the code

Getting jORM to a public maven repo is one of the items on the timeline of the project. For now clone the git repo to get a fresh copy!

    > git clone git://github.com/jajja/jorm.git
    > cd jorm
    > git checkout 1.0.1
    > mvn install

Then include the dependency to jORM in any project you are working on that needs a lightweight ORM.

    <dependency>
        <groupId>com.jajja</groupId>
        <artifactId>jorm</artifactId>
        <version>1.0.1</version>
    </dependency>

Now that you've got the code, let's see if we cannot conjure some cheap tricks!

### Configuring database

The database abstraction in jORM needs a `javax.sql.DataSource` data source. One recommended implementation is the Tomcat JDBC Connection Pool.

    DataSource moriaDataSource = new DataSource();
    moriaDataSource.setDriverClassName("org.postgresql.Driver");
    moriaDataSource.setUrl("jdbc:postgresql://localhost:5432/moria");
    moriaDataSource.setUsername("gandalf");
    moriaDataSource.setPassword("mellon");
    
    Database.configure("moria", moriaDataSource);

This will configure the pooled data source as a named database. For all of those who prefer Spring Beans this can be achieved through a singleton factory method.

    <bean id="moriaDataSource" class="org.apache.tomcat.jdbc.pool.DataSource" destroy-method="close">
        <property name="driverClassname" value="org.postgresql.Driver" />
        <property name="url" value="jdbc:postgresql://localhost:5432/moria" />
        <property name="username" value="gandalf" />
        <property name="password" value="mellon" />
    </bean>

    <bean class="com.jajja.jorm.Database" factory-method="get">
        <property name="dataSources">
            <map>
                <entry key="moria" value-ref="moriaDataSource" />
            </map>
        </property>
    </bean>

### Using databases

All database queries in jORM are executed through a thread local transaction. The first query begins the transaction. After that the transaction can be committed or closed, which implicitly rolls back the transaction.

    Transaction transaction = Database.open("moria");
    try {
        transaction.select("UPDATE goblins SET mindset = 'provoked' RETURNING *");
        transaction.commit();
    } catch (SQLException e) {
        // handle e
    } finally {
        transaction.close();
    }

The database has a shorthand to the thread local transactions. The above can also be expressed as below.

    try {
        Database.open("moria").select("UPDATE goblins SET mindset = 'provoked' RETURNING *");
        Database.commit("moria");
    } catch (SQLException e) {
        // handle e
    } finally {
        Database.close("moria");
    }

If you are using multiple databases it may be a good idea to close all thread local transactions at the end of execution. This can be done by a single call.

    Database.close();
    
Maybe you were interested in something more than executing generic queries? Let's map a table!

### Mapping tables

In order to map a table we need to get an idea of how it is declared. Imagine a table was created using the following statement.

    CREATE TABLE goblins (
        id          serial    NOT NULL,
        tribe_id    int       NOT NULL    REFERENCES tribes(id),
        name        varchar   NOT NULL    DEFAULT 'Azog', 
        mindset     varchar,
        PRIMARY KEY (id),
        UNIQUE (tribe_id, name)
    );

Tables are mapped by records with a little help by the `@Jorm` annotation. Records bind to the tread local transactions defined by the `database` attribute. The `table` attribute defines the mapped table, and the `id` attribute provides primary key functionality.

    @Jorm(database="moria", table="goblins", id="id")
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
    }

Such records can be automatically generated by the `Generator` class. Note that the `Goblin#getTribe()` and `Goblin#setTribe()` methods refers to the `tribe_id` field of the mapped `Goblin` record, but `Tribe` record is also cached for subsequent references. Thus simple foreign keys can be mapped, but how would a tribe look?

    @Jorm(database="moria", table="tribes", id="id")
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
            return findReferences(Goblin.class, "id");
        }
    }

There is no default implementation of `Tribe#setGoblins(List<Goblin>)`. This is not because it is impossible to implement using jORM, but because at this point jORM makes no claim at providing a proper cache for one-to-many relations. There is however a cache implementation for records that could be used for methods like `Tribe#getGoblins()`. For now we'll just let it use a query for each access, and we'll get back to caching strategies.

Did you notice the `UNIQUE` constraint on goblins? It can be used to provide convenient methods for queries on goblins.

    public static Goblin findByTribeAndName(Tribe tribe, String name) throws SQLException {
        return find(Goblin.class, new Column("tribe_id", tribe), new Column("name", name));
    }

If you prefer the write SQL this can also be achieved through manual queries.

    public static Goblin findByTribeAndName(Tribe tribe, String name) throws SQLException {
        return Record.select(Goblin.class, "SELECT * FROM goblins WHERE tribe_id = #1:id# AND name = #2#", tribe, name);
    }

This should be where you've caught the glimpse of a tip of an iceberg, and should ask yourself. What else is there?

### Immutable fields

Sometimes a field in a record mapped from a table could just as well be immutable, such as `left_at` described in the following SQL create statement. Goblin litter is left at exactly one time, never picked up and never left again.

    CREATE TABLE litters (
        id          serial    NOT NULL,
        goblin_id   int       NOT NULL    REFERENCES goblins(id),
        stench      float     NOT NULL    CHECK (stench BETWEEN 0 AND 1),
        left_at     timestamp NOT NULL    DEFAULT now(),
        PRIMARY KEY (id)
    );

Marking immutability for fields can be done by defining the `immutable` attribute in the `@Jorm` mapping. 

    @Jorm(database="moria", table="litters", immutable={"left_at"})
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
        public Float getStench() {
            return get("stench", Float.class);
        }
        public void setStench(Float stench) {
            set("stench", stench);
        }
        public java.sql.Timestamp getLeftAt() {
            return get("left_at", java.sql.Timestamp.class);
        }
    }

The `left_at` column will never change even if an explicit call to `Record#set(String, Object)` has been made.

### Rebranded SQL exceptions

If you have been wondering why the previous example had a check condition on `stench` in the create statement of `litters`, you are about to find out. The records rebrand `SQLException` through dialect specific adaptions, classifying known errors with specific types.

    try {
        litter.setStench(2);    // CHECK (stench BETWEEN 0 AND 1),  
        litter.save();
    } catch (CheckViolationException e) {
        // handle exception
    }

There are four specific types of exceptions and one generic base exception. If this sounds interesting chek out the section about exception handling.

### Queries as fields

Sometimes it can be convenient to have the ability to set fields of records to database queries. One of the most frequent queries usable in this context is `now()`, but it can be any valid query resulting in one row and one column. Let's extend `Goblin` with some random functionality!

    public void relieve() {
        Litter litter = new Litter();
        litter.set("stench", build("random() * 0.9")) ;
        litter.setGoblin(this);
    }

The `Record#build(String, Object...)` method provides a query usable as a field value. Note that the goblin instance needs to be saved before the actual value can be accessed!


## Queries and SQL markup

Queries are expressed in a SQL with hash-markup. References to parameters are enclosed by two hashses (#), and use numbers to address parameters in order of appearance.

    Record.select("SELECT * FROM foo WHERE bar < #1# AND #1# < baz AND baz < #2# ", 10, 100);
    
Quaries are implicitly created by `Record#select(String, Object...)`, but can also be explicitly created.

    Transaction transaction = Database.open("bar");
    Dialect dialect = transaction.getDialect();
    Query query = new Query(dialect, "SELECT * FROM foo WHERE bar < #1# AND #1# < baz AND baz < #2# ", 10, 100);
    
Instances of `Record` wrap dialect retrieval in `Record#build(String, Object...)`, as a syntactic sugar to build queries shown in the previous section 'Queries as fields'.

###Tokens

    #1#     - parameter 1, quoted as value
    #:1#    - parameter 1, quoted as identifier
    #!1#    - parameter 1, not quoted!

### Escaping

Hashes (#) can be quoted by double-hashing, i.e ##, ? cannot be escaped properly due to design flaws in the JDBC.

    Record.select("SELECT 1 ## 2");     // = "SELECT 1 # 2"

### Java arrays

    Integer[] ids = new Integer[]{1, 2, 3};
    Record.select("SELECT * FROM foo WHERE id IN (#1#)", ids);
    Record.select("SELECT * FROM foo WHERE id IN (#!1#)", ids); // No quoting performed!

### Java collections

    List<String> names = new LinkedList<String>();
    names.add("John");
    names.add("Doe");
    Record.select("SELECT * FROM foo WHERE names IN (#1#)", names);

### Java collections

    List<Record> records = new LinkedList<Record>();
    records.add(someRecord1);
    records.add(someRecord2);
    Record.select("SELECT * FROM foo WHERE names IN (#1:some_column#)", records);

### Java maps

    Map<String, Integer> map = new HashMap<String, Integer>();
    map.put("foo", 5);
    map.put("bar", 3);
    Record.select("SELECT * FROM foobars WHERE foo_id = #1:foo# OR bar_id = #1:bar#", map);

### jORM tables
  
    Record.select("SELECT * FROM #1# WHERE id = 5", table(Goblin.class));   // Modifier ignored, tables are always quoted as a identifiers

### jORM symbols

    Record.select("SELECT * FROM foo WHERE #1# = 5", Symbol.get("id")); // Modifier ignored, Symbols are always quoted as identifiers

### Nested jORM queries

    Transaction transaction = Database.open("moria");
    Dialect dialect = transaction.getDialect();
    Query subQuery = new Query(dialect, "SELECT id FROM bar WHERE baz LIKE #1#", "%moo%");
    Query query = new Query(dialect, "SELECT * FROM foo WHERE bar_id IN (#1#)", subQuery);


## Transaction lifecycle

Transactions opened to a database are thread local and there is atmost one transaction for each named database (which may be exploited by providing more than one name for a single logical database). These transactions need to be closed at the logical end of execution. A web server may typically reuse threads for subsequent request, making the end of a request the logical end of execution. Any other application  reusing threads need the corresponding adaptions described throughout this section.

### Transaction access

The safest way of ensuring transaction lifecycle through an application reusing threads is to manually close the connection within the scope of the change to the database.

    Transaction transaction = Database.open("moria");
    try {
        // do stuff 
        transaction.commit();
    } catch (SQLException e) {
        // handle exception
    } finally {
        transaction.close();
    }

However, code modularization can make passing of references to transactions cumbersome. For this reason transactions can be uniquely accessed through the named database, in the context of the current thread.

    Database.open("moria");
    try {
        // do stuff
        Database.commit("moria");
    } catch (SQLException e) {
        // handle exception
    } finally {
        Database.close("moria");
    }

There is conveniently wrapped by static methods of the record, using `@Jorm` annotations to define named datbases.

    Record.open(Goblin.class);
    try {
        // do stuff
        Record.commit(Goblin.class);
    } catch (SQLException e) {
        // handle exception
    } finally {
        Record.close(Goblin.class);
    }

Transactions are even available through record instances, which cane sometimes be convenient. Note that any record may act as reference to the thread local transaction possibly shared by multiple records.

    Record context = null
    try {
        // do stuff
        if (context != null) context.commit();
    } catch (SQLException e) {
        // handle exception
    } finally {
        if (context != null) context.close();
        // else what?
    }

This last example points to the fact that execution is not always predictable. In most applications a runtime exception may occurr depending on user input or other uncontrolled circumstances.

    try {
        // do unpredictable stuff
    } finally {
        Database.close();
        // release thread
    }

The above example closes all thread local transactions are releasing the current thread to its imagined thread pool. It may be considered good practice to build this into your applications execution lifecycle.

### Transaction savepoints

For some types of problems it may be beneficiary to make use of savepoints. For this reason the savepoints of JDBC are wrapped by transactions.

    Transation transaction = Database.open("moria");
    try {
        Goblin azog = new Goblin("Azog");
        Tribe tribe = Record.findById(Tribe.class, 1);
        azog.setTribe(tribe);
        Savepoint savepoint = transaction.save();
        try {
            azog.save();
            transaction.release(savepoint);
        } catch (UniqueViolationException) {
            transaction.rollback(savepoint);
            azog = findByTribeAndName(tribe, "Azog");
        }
        // do more stuff
        transaction.commit();
    } catch (SQLException e) {
        // handle exception
    } finally {
        transaction.close();
    }

Note that not every database engine supports release of savepoints. MSSQL only supports rollback, and there is more to read about engine/driver specific behaviour in the next section!

## Database engines

jORM has been tested on Postgres, MySQL and MSSQL. If you have a license to a dababase engine and would like to contribute, please feel free to contact the authors.

### Is jORM database agnostic?

The not so simple answer is _yes and no_! The library should be able to execute database agnostically using the JDBC as abstraction. However, jORM takes advantage of engine specific functionality. For instance Postgres is the only database engine supporting the `RETURNING` clause (that we've stumbled across so far), and thus Postgres integrations using jORM do not need to query for results explicitly after an insert or update. 

There are also targeted fixes patching unexpected behaviour in specific JDBC-implementations. Any database engines that have not been validated might just as well contain similar problems in their respecitve implementations of the JDBC.

One thing that will differ if using another database egine is a rebrand strategy for `SQLException` that jORM use to classify different types of SQL errors. These are only available for Postgres, MySQL and MSSQL at this moment. More about this will appear in the nondistant future.


## To be continued..

This README will be updated with more advanced and in-depth examples of how to best make use of jORM. One of the first things on our TODO list is to document the SQL markup syntax for queries through records and transactions properly.

[1]: http://www.jajja.com "Jajja Communications AB"
