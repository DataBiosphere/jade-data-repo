package bio.terra.stairway;


import bio.terra.category.StairwayUnit;
import bio.terra.configuration.StairwayJdbcConfiguration;
import bio.terra.stairway.exception.FlightNotFoundException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;
import java.io.PrintWriter;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;

@RunWith(SpringRunner.class)
@SpringBootTest
@Category(StairwayUnit.class)
public class ScenarioTest {
    private Stairway stairway;
    private Logger logger = LoggerFactory.getLogger("bio.terra.stairway");

    @Autowired
    private StairwayJdbcConfiguration jdbcConfiguration;

    @Before
    public void setup() {
        stairway = TestUtil.setupStairway(jdbcConfiguration);
    }

    @Test
    public void simpleTest() {
        // Generate a unique filename
        String filename = makeFilename();
        logger.debug("Filename: " + filename);

        // Submit the test flight
        FlightMap inputParameters = new FlightMap();
        inputParameters.put("filename", filename);
        inputParameters.put("text", "testing 1 2 3");

        String flightId = stairway.submit(TestFlight.class, inputParameters);
        logger.debug("Submitted flight id: " + flightId);

        // Test for done
        boolean done = stairway.isDone(flightId);
        logger.debug("Flight done: " + done);

        // Wait for done
        stairway.waitForFlight(flightId);
        FlightState result = stairway.getFlightState(flightId);
        Assert.assertThat(result.getFlightStatus(), is(FlightStatus.SUCCESS));
        Assert.assertFalse(result.getErrorMessage().isPresent());

        // Should be released
        try {
            stairway.waitForFlight(flightId);
        } catch (FlightNotFoundException ex) {
            Assert.assertThat(ex.getMessage(), containsString(flightId));
        }
    }

    @Test
    public void testFileExists() throws Exception {
        // Generate a filename and create the file
        String filename = makeExistingFile();

        // Submit the test flight
        FlightMap inputParameters = new FlightMap();
        inputParameters.put("filename", filename);
        inputParameters.put("text", "testing 1 2 3");

        String flightId = stairway.submit(TestFlight.class, inputParameters);

        // Poll waiting for done
        while (!stairway.isDone(flightId)) {
            Thread.sleep(1000);
        }

        // Handle results
        FlightState result = stairway.getFlightState(flightId);
        Assert.assertThat(result.getFlightStatus(), is(FlightStatus.ERROR));
        Assert.assertTrue(result.getErrorMessage().isPresent());

        // The error text thrown by TestStepExistence
        Assert.assertThat(result.getErrorMessage().get(), containsString("already exists"));
    }

    @Test
    public void testUndo() throws Exception {
        // The plan is:
        // > pre-create abcd.txt
        // > random file
        //  - step 1 file exists random file
        //  - step 2 create random file
        //  - step 3 file exists pre-created file (will fail)
        //  - step 4 create pre-created file (should not get here)

        // Generate a filename and create the file
        String existingFilename = makeExistingFile();

        // Generate non-existent filename
        String filename = makeFilename();

        // Submit the test flight
        FlightMap inputParameters = new FlightMap();
        inputParameters.put("filename", filename);
        inputParameters.put("existingFilename", existingFilename);
        inputParameters.put("text", "testing 1 2 3");

        String flightId = stairway.submit(TestFlightUndo.class, inputParameters);

        // Wait for done
        stairway.waitForFlight(flightId);
        FlightState result = stairway.getFlightState(flightId);
        Assert.assertThat(result.getFlightStatus(), is(FlightStatus.ERROR));
        Assert.assertTrue(result.getErrorMessage().isPresent());
        Assert.assertThat(result.getErrorMessage().get(), containsString("already exists"));

        // We expect the non-existent filename to have been deleted
        File file = new File(filename);
        Assert.assertFalse(file.exists());

        // We expect the existent filename to still be there
        file = new File(existingFilename);
        Assert.assertTrue(file.exists());
    }

    private String makeExistingFile() throws Exception {
        // Generate a filename and create the file
        String existingFilename = makeFilename();
        PrintWriter writer = new PrintWriter(existingFilename, "UTF-8");
        writer.println("abcd");
        writer.close();
        logger.debug("Existing Filename: " + existingFilename);
        return existingFilename;
    }

    private String makeFilename() {
        return "/tmp/test." + UUID.randomUUID().toString() + ".txt";
    }

}
