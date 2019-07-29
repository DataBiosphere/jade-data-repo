package bio.terra.stairway;

import bio.terra.category.StairwayUnit;
import bio.terra.configuration.StairwayJdbcConfiguration;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

import static bio.terra.stairway.TestUtil.dubValue;
import static bio.terra.stairway.TestUtil.errString;
import static bio.terra.stairway.TestUtil.fkey;
import static bio.terra.stairway.TestUtil.flightId;
import static bio.terra.stairway.TestUtil.ikey;
import static bio.terra.stairway.TestUtil.intValue;
import static bio.terra.stairway.TestUtil.skey;
import static bio.terra.stairway.TestUtil.strValue;
import static bio.terra.stairway.TestUtil.wfkey;
import static bio.terra.stairway.TestUtil.wikey;
import static bio.terra.stairway.TestUtil.wskey;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(StairwayUnit.class)
public class DatabaseOperationsTest {
    private PoolingDataSource<PoolableConnection> dataSource;

    @Autowired
    private StairwayJdbcConfiguration jdbcConfiguration;

    @Before
    public void setup() {
        dataSource = TestUtil.setupDataSource(jdbcConfiguration);
    }

    @Test
    public void basicsTest() throws Exception {
        Database database = createDatabase(true);

        FlightMap inputs = new FlightMap();
        inputs.put(ikey, intValue);
        inputs.put(skey, strValue);
        inputs.put(fkey, dubValue);

        FlightContext flightContext = new FlightContext(inputs, "notArealClass");
        flightContext.setFlightId(flightId);

        database.submit(flightContext);

        // Use recover to retrieve the internal state of the flight
        List<FlightContext> flightList = database.recover();
        Assert.assertThat(flightList.size(), is(equalTo(1)));
        FlightContext recoveredFlight = flightList.get(0);

        Assert.assertThat(recoveredFlight.getFlightId(), is(equalTo(flightContext.getFlightId())));
        Assert.assertThat(recoveredFlight.getFlightClassName(), is(equalTo(flightContext.getFlightClassName())));
        Assert.assertThat(recoveredFlight.getStepIndex(), is(equalTo(0)));
        Assert.assertThat(recoveredFlight.isDoing(), is(true));
        Assert.assertThat(recoveredFlight.getResult().isSuccess(), is(true));
        Assert.assertThat(recoveredFlight.getFlightStatus(), is(FlightStatus.RUNNING));

        FlightMap recoveredInputs = recoveredFlight.getInputParameters();
        checkInputs(recoveredInputs);

        // Use getFlightState to retrieve the externally visible state of the flight
        FlightState flightState = database.getFlightState(flightId);
        checkRunningFlightState(flightState);
        FlightMap stateInputs = flightState.getInputParameters();
        checkInputs(stateInputs);

        flightContext.setStepIndex(1);
        database.step(flightContext);

        Thread.sleep(1000);

        flightContext.setDoing(false);
        flightContext.setResult(new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL,
                new IllegalArgumentException(errString)));
        flightContext.setStepIndex(2);
        flightContext.setDoing(false);
        flightContext.getWorkingMap().put(wfkey, dubValue);
        flightContext.getWorkingMap().put(wikey, intValue);
        flightContext.getWorkingMap().put(wskey, strValue);

        database.step(flightContext);

        flightList = database.recover();
        Assert.assertThat(flightList.size(), is(equalTo(1)));
        recoveredFlight = flightList.get(0);
        Assert.assertThat(recoveredFlight.getStepIndex(), is(equalTo(2)));
        Assert.assertThat(recoveredFlight.isDoing(), is(false));
        Assert.assertThat(recoveredFlight.getResult().isSuccess(), is(false));
        Assert.assertThat(recoveredFlight.getResult().getException().get().toString(), containsString(errString));
        Assert.assertThat(recoveredFlight.getFlightStatus(), is(FlightStatus.RUNNING));

        FlightMap recoveredWork = recoveredFlight.getWorkingMap();
        checkOutputs(recoveredWork);

        flightState = database.getFlightState(flightId);
        checkRunningFlightState(flightState);
        stateInputs = flightState.getInputParameters();
        checkInputs(stateInputs);

        flightContext.setFlightStatus(FlightStatus.ERROR);

        database.complete(flightContext);

        flightList = database.recover();
        Assert.assertThat(flightList.size(), is(equalTo(0)));

        List<FlightState> flightStateList = database.getFlights(0, 100);
        Assert.assertThat(flightStateList.size(), is(1));
        flightState = flightStateList.get(0);
        Assert.assertThat(flightState.getFlightId(), is(flightId));
        Assert.assertThat(flightState.getFlightStatus(), is(FlightStatus.ERROR));
        Assert.assertTrue(flightState.getResultMap().isPresent());
        Assert.assertTrue(flightState.getException().isPresent());

        FlightMap outputParams = flightState.getResultMap().get();
        checkOutputs(outputParams);
        Assert.assertThat(flightState.getException().get().toString(), containsString(errString));
    }

    private Database createDatabase(boolean forceCleanStart) {
        return new Database(dataSource, forceCleanStart);
    }

    private void checkRunningFlightState(FlightState flightState) {
        Assert.assertThat(flightState.getFlightId(), is(flightId));
        Assert.assertThat(flightState.getFlightStatus(), is(FlightStatus.RUNNING));
        Assert.assertFalse(flightState.getCompleted().isPresent());
        Assert.assertFalse(flightState.getResultMap().isPresent());
        Assert.assertFalse(flightState.getException().isPresent());
    }

    private void checkInputs(FlightMap inputMap) {
        Assert.assertThat(inputMap.get(fkey, Double.class), is(equalTo(dubValue)));
        Assert.assertThat(inputMap.get(skey, String.class), is(equalTo(strValue)));
        Assert.assertThat(inputMap.get(ikey, Integer.class), is(equalTo(intValue)));
    }

    private void checkOutputs(FlightMap outputMap) {
        Assert.assertThat(outputMap.get(wfkey, Double.class), is(equalTo(dubValue)));
        Assert.assertThat(outputMap.get(wskey, String.class), is(equalTo(strValue)));
        Assert.assertThat(outputMap.get(wikey, Integer.class), is(equalTo(intValue)));
    }
}
