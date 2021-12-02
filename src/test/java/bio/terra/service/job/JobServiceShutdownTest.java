package bio.terra.service.job;

import static org.junit.Assert.assertTrue;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Connected;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.Stairway;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@SpringBootTest
@Category(Connected.class)
@EmbeddedDatabaseTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class JobServiceShutdownTest {
  private static final Logger logger = LoggerFactory.getLogger(JobServiceShutdownTest.class);

  private final AuthenticatedUserRequest testUser =
      AuthenticatedUserRequest.builder()
          .setSubjectId("StairwayUnit")
          .setEmail("stairway@unit.com")
          .setToken("token")
          .build();

  @Autowired private JobService jobService;

  @Autowired private ApplicationConfiguration appConfig;

  @Test
  public void testShutdown() throws Exception {
    // Segregated into its own test, since the Stairway object is not usable after it completes
    // scenario:
    //  - start a thread that creates one flight every X second; keeping track of the flights
    //     - each flight sleeps Y secs and then returns; we want to get a backlog of flights in the
    // queue
    //    when thread catches JobServiceShutdown exception, it stops launching and returns its
    // flight list
    //  - sleep Z seconds to let the system fill up
    //  - call shutdown
    //  - validate that flights are either SUCCESS or READY

    // Test Control Parameters
    final int flightPerSecond = 1;
    final int flightWaitSeconds = 30;

    int maxStairwayThreads = appConfig.getMaxStairwayThreads();
    logger.info("maxStairwayThreads: " + maxStairwayThreads);

    JobTestShutdownFlightLauncher launcher =
        new JobTestShutdownFlightLauncher(flightPerSecond, flightWaitSeconds, testUser, jobService);

    Thread launchThread = new Thread(launcher);
    launchThread.start();

    // Build a backlog in the queue
    TimeUnit.SECONDS.sleep(2 * maxStairwayThreads);
    jobService.shutdown();

    launchThread.join();
    List<String> flightIdList = launcher.getFlightIdList();
    Stairway stairway = jobService.getStairway();

    for (String flightId : flightIdList) {
      FlightState state = stairway.getFlightState(flightId);
      FlightStatus status = state.getFlightStatus();
      logger.info("Flightid: " + flightId + "; status: " + status);
      assertTrue(status == FlightStatus.SUCCESS || status == FlightStatus.READY);
    }
  }
}
