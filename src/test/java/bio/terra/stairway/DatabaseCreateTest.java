package bio.terra.stairway;

import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;

@TestPropertySource(locations = "file://${HOME}/drmetadata_test.properties")
@RunWith(SpringRunner.class)
@SpringBootTest
public class DatabaseCreateTest {
    PoolingDataSource<PoolableConnection> dataSource;

    @Autowired
    MetadataJdbcConfiguration jdbcConfiguration;

    @Before
    public void setup() {
        Properties props = new Properties();
        props.setProperty("user",jdbcConfiguration.getUser());
        props.setProperty("password",jdbcConfiguration.getPassword());

        ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(jdbcConfiguration.getUri(), props);

        PoolableConnectionFactory poolableConnectionFactory =
                new PoolableConnectionFactory(connectionFactory, null);

        ObjectPool<PoolableConnection> connectionPool =
                new GenericObjectPool<>(poolableConnectionFactory);

        poolableConnectionFactory.setPool(connectionPool);

        dataSource = new PoolingDataSource<>(connectionPool);
    }

    @Test
    public void createTest() throws Exception {
        // Start clean
        Database database = createDatabase(true);
        String createtime = getCreateTime(database);

        database = createDatabase(false);

        // Dirty start should not overwrite the database
        String createtime2 = getCreateTime(database);
        Assert.assertThat(createtime2, is(equalTo(createtime)));

        database = createDatabase(true);

        // Clean start should overwrite the database
        String createtime3 = getCreateTime(database);
        Assert.assertThat(createtime3, not(equalTo(createtime)));
    }

    private Database createDatabase(boolean forceCleanStart) {
        Database database = new DatabaseBuilder()
                .dataSource(dataSource)
                .forceCleanStart(forceCleanStart)
                .build();
        Assert.assertThat(database.getFlightLogTableName(), is(equalTo(Database.FLIGHT_LOG_TABLE)));
        Assert.assertThat(database.getFlightTableName(), is(equalTo(Database.FLIGHT_TABLE)));
        Assert.assertThat(database.getFlightVersionTableName(), is(equalTo(Database.FLIGHT_VERSION_TABLE)));
        return database;
    }


    private String getCreateTime(Database database) throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            return database.readDatabaseSchemaCreateTime(statement);
        }
    }

}
