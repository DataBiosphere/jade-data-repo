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
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;

/**
 * The recovery tests are a bit tricky to write, because we need to simulate a failure, create a new
 * stairway and successfully run recovery.
 *
 * The first part of the solution is to build a way to stop a flight at a step to control where the failure
 * happens, and to not stop at that step the second time through. To do the coordination, we make a singleton
 * class TestStopController that holds a volatile variable. We make an associated step class called
 * TestStepStop that finds the stop controller, reads the variable, and obeys the variable's instruction.
 * Here are the cases:
 *  - stop controller = 0 means to sit in a sleep loop forever (well, an hour)
 *  - stop controller = 1 means to skip sleeping
 *
 * The success recovery test works like this:
 *  1. Set stop controller = 0
 *  2. Create stairway1 and launch a flight
 *  3. Flight runs steps up to the stop step
 *  4. Stop step goes to sleep
 * At this point, the database looks like we have a partially complete, succeeding flight.
 * Now we can test recovery:
 *  5. Set stop controller = 1
 *  6. Create stairway2 with the same database. That will trigger recovery.
 *  7. Flight re-does the stop step. This time stop controller is 2 and we skip the sleeping
 *  8. Flight completes successfully
 * At this point: we can evaluate the results of the recovered flight to make sure it worked right.
 * When the test is torn down, the sleeping thread will record failure in the database, so you cannot
 * use state there at this point for any validation.
 *
 * The undo recovery test works by introducint TestStepTriggerUndo that will
 * set the stop controller from 0 and trigger undo. Then the TestStepStop will sleep on the undo
 * path simulating a failure in that direction.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(StairwayUnit.class)
public class RecoveryTest {
    private PoolingDataSource<PoolableConnection> dataSource;
    private ExecutorService executorService;

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

        executorService = Executors.newFixedThreadPool(3);
    }

    @Test
    public void successTest() throws Exception {
        // Start with a clean and shiny database environment.
        Stairway stairway1 = new Stairway(executorService, null);
        stairway1.initialize(dataSource, true);

        FlightMap inputs = new FlightMap();

        Integer initialValue = Integer.valueOf(0);
        inputs.put("initialValue", initialValue);

        TestStopController.setControl(0);
        String flightId = stairway1.submit(TestFlightRecovery.class, inputs);

        // Allow time for the flight thread to go to sleep
        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }

        Assert.assertThat(stairway1.isDone(flightId), is(equalTo(false)));

        // Simulate a restart with a new thread pool and stairway. Set control so this one does not sleep
        TestStopController.setControl(1);
        Stairway stairway2 = new Stairway(executorService, null);
        stairway2.initialize(dataSource, false);

        // Wait for recovery to complete
        stairway2.waitForFlight(flightId);
        FlightState result = stairway2.getFlightState(flightId);
        Assert.assertThat(result.getFlightStatus(), is(equalTo(FlightStatus.SUCCESS)));
        Assert.assertTrue(result.getResultMap().isPresent());
        Integer value = result.getResultMap().get().get("value", Integer.class);
        Assert.assertThat(value, is(equalTo(2)));
    }

    @Test
    public void undoTest() throws Exception {
        // Start with a clean and shiny database environment.
        Stairway stairway1 = new Stairway(executorService, null);
        stairway1.initialize(dataSource, true);

        FlightMap inputs = new FlightMap();
        Integer initialValue = Integer.valueOf(2);
        inputs.put("initialValue", initialValue);

        // We don't want to stop on the do path; the undo trigger will set the control to 0 and put the flight to sleep
        TestStopController.setControl(1);
        String flightId = stairway1.submit(TestFlightRecoveryUndo.class, inputs);

        // Allow time for the flight thread to go to sleep
        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }

        Assert.assertThat(stairway1.isDone(flightId), is(equalTo(false)));

        // Simulate a restart with a new thread pool and stairway. Reset control so this one does not sleep
        TestStopController.setControl(1);
        Stairway stairway2 = new Stairway(executorService, null);
        stairway2.initialize(dataSource, false);

        // Wait for recovery to complete
        stairway2.waitForFlight(flightId);
        FlightState result = stairway2.getFlightState(flightId);
        Assert.assertThat(result.getFlightStatus(), is(equalTo(FlightStatus.ERROR)));
        Assert.assertTrue(result.getException().isPresent());
        // The exception is thrown by TestStepTriggerUndo
        Assert.assertThat(result.getException().get().getMessage(),
            is(containsString("TestStepTriggerUndo")));

        Assert.assertTrue(result.getResultMap().isPresent());
        Integer value = result.getResultMap().get().get("value", Integer.class);
        Assert.assertThat(value, is(equalTo(2)));
    }

}
