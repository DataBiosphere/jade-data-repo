package bio.terra.stairway;

import bio.terra.category.StairwayUnit;
import bio.terra.configuration.StairwayJdbcConfiguration;
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
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

@RunWith(SpringRunner.class)
@SpringBootTest
@Category(StairwayUnit.class)
public class DatabaseOperationsTest {
    private PoolingDataSource<PoolableConnection> dataSource;

    @Autowired
    private StairwayJdbcConfiguration jdbcConfiguration;

    @Before
    public void setup() {
        Properties props = new Properties();
        props.setProperty("user", jdbcConfiguration.getUsername());
        props.setProperty("password", jdbcConfiguration.getPassword());

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
        Integer intValue = Integer.valueOf(22);
        String strValue = "testing 1 2 3";
        Double dubValue = new Double(Math.PI);
        String errString = "Something bad happened";

        Database database = createDatabase(true);

        FlightMap inputs = new FlightMap();
        inputs.put("ikey", intValue);
        inputs.put("skey", strValue);
        inputs.put("fkey", dubValue);

        FlightContext flightContext = new FlightContext(inputs, "notArealClass");
        flightContext.setFlightId("aaa111");

        database.submit(flightContext);

        List<FlightContext> flightList = database.recover();
        Assert.assertThat(flightList.size(), is(equalTo(1)));
        FlightContext recoveredFlight = flightList.get(0);

        Assert.assertThat(recoveredFlight.getFlightId(), is(equalTo(flightContext.getFlightId())));
        Assert.assertThat(recoveredFlight.getFlightClassName(), is(equalTo(flightContext.getFlightClassName())));
        Assert.assertThat(recoveredFlight.getStepIndex(), is(equalTo(0)));
        Assert.assertThat(recoveredFlight.isDoing(), is(true));
        Assert.assertThat(recoveredFlight.getResult().isSuccess(), is(true));

        FlightMap recoveredInputs = recoveredFlight.getInputParameters();
        Assert.assertThat(recoveredInputs.get("fkey", Double.class), is(equalTo(dubValue)));
        Assert.assertThat(recoveredInputs.get("skey", String.class), is(equalTo(strValue)));
        Assert.assertThat(recoveredInputs.get("ikey", Integer.class), is(equalTo(intValue)));

        flightContext.setStepIndex(1);
        database.step(flightContext);

        Thread.sleep(1000);

        flightContext.setDoing(false);
        flightContext.setResult(new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL,
                new IllegalArgumentException(errString)));
        flightContext.setStepIndex(2);
        flightContext.setDoing(false);
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

        FlightMap recoveredWork = recoveredFlight.getWorkingMap();
        Assert.assertThat(recoveredWork.get("wfkey", Double.class), is(equalTo(dubValue)));
        Assert.assertThat(recoveredWork.get("wskey", String.class), is(equalTo(strValue)));
        Assert.assertThat(recoveredWork.get("wikey", Integer.class), is(equalTo(intValue)));

        database.complete(flightContext);

        flightList = database.recover();
        Assert.assertThat(flightList.size(), is(equalTo(0)));
    }

    private Database createDatabase(boolean forceCleanStart) {
        return new Database(dataSource, forceCleanStart);
    }

}
