package bio.terra.stairway;


import bio.terra.stairway.exception.FlightNotFoundException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.PrintWriter;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.CoreMatchers.containsString;

public class ScenarioTest {
    private ExecutorService executorService;
    private Stairway stairway;

    @Before
    public void setup() {
        executorService = Executors.newFixedThreadPool(2);
        stairway = new Stairway(executorService);
    }

    @Test
    public void simpleTest() {
        // Generate a unique filename
        String filename = makeFilename();
        System.out.println("Filename: " + filename);

        // Submit the test flight
        SafeHashMap inputParameters = new SafeHashMap();
        inputParameters.put("filename", filename);
        inputParameters.put("text", "testing 1 2 3");

        String flightId = stairway.submit(TestFlight.class, inputParameters);
        System.out.println("Submitted flight id: " + flightId);

        // Test for done
        boolean done = stairway.isDone(flightId);
        System.out.println("Flight done: " + done);

        // Wait for done
        FlightResult result = stairway.getResult(flightId);
        Assert.assertTrue(result.isSuccess());
        Assert.assertFalse(result.getThrowable().isPresent());

        // Should be released
        try {
            stairway.getResult(flightId);
        } catch (FlightNotFoundException ex) {
            Assert.assertThat(ex.getMessage(), containsString(flightId));
        }
    }

    @Test
    public void testFileExists() throws Exception {
        // Generate a filename and create the file
        String filename = makeExistingFile();

        // Submit the test flight
        SafeHashMap inputParameters = new SafeHashMap();
        inputParameters.put("filename", filename);
        inputParameters.put("text", "testing 1 2 3");

        String flightId = stairway.submit(TestFlight.class, inputParameters);
        System.out.println("Submitted flight id: " + flightId);

        // Test for done
        boolean done = stairway.isDone(flightId);
        System.out.println("Flight done: " + done);

        // Wait for done
        FlightResult result = stairway.getResult(flightId);
        System.out.println("Flight result: " + result);
        Assert.assertFalse(result.isSuccess());
        Optional<Throwable> throwable = result.getThrowable();
        Assert.assertTrue(throwable.isPresent());
        // The exception is thrown by TestStepExistence
        Assert.assertTrue(throwable.get() instanceof IllegalArgumentException);
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
        SafeHashMap inputParameters = new SafeHashMap();
        inputParameters.put("filename", filename);
        inputParameters.put("existingFilename", existingFilename);
        inputParameters.put("text", "testing 1 2 3");

        String flightId = stairway.submit(TestFlightUndo.class, inputParameters);

        // Wait for done
        FlightResult result = stairway.getResult(flightId);
        Assert.assertFalse(result.isSuccess());
        Optional<Throwable> throwable = result.getThrowable();
        Assert.assertTrue(throwable.isPresent());
        // The exception is thrown by TestStepExistence
        Assert.assertTrue(throwable.get() instanceof IllegalArgumentException);

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
        System.out.println("Existing Filename: " + existingFilename);
        return existingFilename;
    }

    private String makeFilename() {
        return "/tmp/test." + UUID.randomUUID().toString() + ".txt";
    }


}
