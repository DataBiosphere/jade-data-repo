package bio.terra.service.job;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.job.exception.JobServiceShutdownException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobTestShutdownFlightLauncher implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(JobTestShutdownFlightLauncher.class);

  private List<String> flightIdList;
  private int flightPerSecond;
  private Integer flightWaitSeconds;
  private JobService jobService;
  private AuthenticatedUserRequest testUser;

  public JobTestShutdownFlightLauncher(
      int flightPerSecond,
      int flightWaitSeconds,
      AuthenticatedUserRequest testUser,
      JobService jobService) {
    this.flightPerSecond = flightPerSecond;
    this.flightWaitSeconds = flightWaitSeconds;
    this.jobService = jobService;
    this.testUser = testUser;

    flightIdList = new LinkedList<>();
  }

  @Override
  public void run() {
    for (int i = 0; true; i++) {
      try {
        String jobid =
            jobService
                .newJob("flight #" + i, JobTestShutdownFlight.class, null, testUser)
                .addParameter("flightWaitSeconds", flightWaitSeconds)
                .submit();
        flightIdList.add(jobid);
        logger.info("Launched flight #" + i + "  jobid: " + jobid);
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
