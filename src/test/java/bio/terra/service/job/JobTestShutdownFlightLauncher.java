package bio.terra.service.job;

import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.job.exception.JobServiceShutdownException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobTestShutdownFlightLauncher implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(JobTestShutdownFlightLauncher.class);

  private final List<String> flightIdList;
  private final int flightPerSecond;
  private final Integer flightWaitSeconds;
  private final JobService jobService;
  private final AuthenticatedUserRequest testUser;

  public JobTestShutdownFlightLauncher(
      int flightPerSecond,
      int flightWaitSeconds,
      AuthenticatedUserRequest testUser,
      JobService jobService) {
    this.flightPerSecond = flightPerSecond;
    this.flightWaitSeconds = flightWaitSeconds;
    this.jobService = jobService;
    this.testUser = testUser;

    flightIdList = new ArrayList<>();
  }

  @Override
  public void run() {
    for (int i = 0; true; i++) {
      try {
        String jobId =
            jobService
                .newJob("flight #" + i, JobTestShutdownFlight.class, null, testUser)
                .addParameter("flightWaitSeconds", flightWaitSeconds)
                .submit();
        flightIdList.add(jobId);
        logger.info("Launched flight #" + i + "  jobId: " + jobId);
        TimeUnit.SECONDS.sleep(flightPerSecond);
      } catch (JobServiceShutdownException ex) {
        logger.info("Caught JobServiceShutdownException");
        break;
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public List<String> getFlightIdList() {
    return flightIdList;
  }
}
