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

import java.util.List;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

// TODO: Tests
// - idempotency of complete
// - scenario tests with the database
// -- success, successful undo, interrupted doing, interrupted undoing

@TestPropertySource(locations = "file://${HOME}/drmetadata_test.properties")
@RunWith(SpringRunner.class)
@SpringBootTest
public class DatabaseOperationsTest {
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
    public void basicsTest() throws Exception {
        Integer intValue = new Integer(22);
        String strValue = "testing 1 2 3";
        Double dubValue = new Double(3.1415);
        String errString = "Something bad happened";

        Database database = createDatabase(true);

        SafeHashMap inputs = new SafeHashMap();
        inputs.put("ikey", intValue);
        inputs.put("skey", strValue);
        inputs.put("fkey", dubValue);

        FlightContext flightContext = new FlightContext(inputs)
                .flightId("aaa111")
                .flightClassName("notArealClass");

        database.submit(flightContext);

        List<FlightContext> flightList = database.recover();
        Assert.assertThat(flightList.size(), is(equalTo(1)));
        FlightContext recoveredFlight = flightList.get(0);

        Assert.assertThat(recoveredFlight.getFlightId(), is(equalTo(flightContext.getFlightId())));
        Assert.assertThat(recoveredFlight.getFlightClassName(), is(equalTo(flightContext.getFlightClassName())));
        Assert.assertThat(recoveredFlight.getStepIndex(), is(equalTo(0)));
        Assert.assertThat(recoveredFlight.isDoing(), is(true));
        Assert.assertThat(recoveredFlight.getResult().isSuccess(), is (true));

        SafeHashMap recoveredInputs = recoveredFlight.getInputParameters();
        Assert.assertThat(recoveredInputs.get("fkey", Double.class), is(equalTo(dubValue)));
        Assert.assertThat(recoveredInputs.get("skey", String.class), is(equalTo(strValue)));
        Assert.assertThat(recoveredInputs.get("ikey", Integer.class), is(equalTo(intValue)));

        flightContext.stepIndex(1);
        database.step(flightContext);

        Thread.sleep(1000);

        flightContext.doing(false);
        flightContext.result(new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL,
                new IllegalArgumentException(errString)));
        flightContext.stepIndex(2).doing(false);
        flightContext.getWorkingMap().put("wfkey", dubValue);
        flightContext.getWorkingMap().put("wikey", intValue);
        flightContext.getWorkingMap().put("wskey", strValue);

        database.step(flightContext);

        flightList = database.recover();
        Assert.assertThat(flightList.size(), is(equalTo(1)));
        recoveredFlight = flightList.get(0);
        Assert.assertThat(recoveredFlight.getStepIndex(), is(equalTo(2)));
        Assert.assertThat(recoveredFlight.isDoing(), is(false));
        Assert.assertThat(recoveredFlight.getResult().isSuccess(), is(false));
        Assert.assertThat(recoveredFlight.getResult().getThrowable().get().toString(), containsString(errString));

        SafeHashMap recoveredWork = recoveredFlight.getWorkingMap();
        Assert.assertThat(recoveredWork.get("wfkey", Double.class), is(equalTo(dubValue)));
        Assert.assertThat(recoveredWork.get("wskey", String.class), is(equalTo(strValue)));
        Assert.assertThat(recoveredWork.get("wikey", Integer.class), is(equalTo(intValue)));

        database.complete(flightContext);

        flightList = database.recover();
        Assert.assertThat(flightList.size(), is(equalTo(0)));
    }

    private Database createDatabase(boolean forceCleanStart) {
        Database database = new DatabaseBuilder()
                .dataSource(dataSource)
                .forceCleanStart(forceCleanStart)
                .build();
        return database;
    }




}
