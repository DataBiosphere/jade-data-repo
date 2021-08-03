package bio.terra.app.utils.startup;

import bio.terra.service.job.JobService;
import org.springframework.context.ApplicationContext;

public final class StartupInitializer {

  private StartupInitializer() {}

  public static void initialize(ApplicationContext applicationContext) {
    // Initialize jobService, stairway, migrate databases, perform recovery
    JobService jobService = (JobService) applicationContext.getBean("jobService");
    jobService.initialize();
  }
}
