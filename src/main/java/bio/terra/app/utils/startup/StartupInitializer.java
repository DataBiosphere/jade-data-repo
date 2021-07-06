package bio.terra.app.utils.startup;

import bio.terra.service.job.JobService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

public final class StartupInitializer {
  private static final Logger logger = LoggerFactory.getLogger(StartupInitializer.class);

  private StartupInitializer() {}

  public static void initialize(ApplicationContext applicationContext) {
    // Initialize jobService, stairway, migrate databases, perform recovery
    JobService jobService = (JobService) applicationContext.getBean("jobService");
    jobService.initialize();
  }
}
