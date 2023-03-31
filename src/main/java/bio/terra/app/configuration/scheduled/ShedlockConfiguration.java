package bio.terra.app.configuration.scheduled;

import bio.terra.app.configuration.DataRepoJdbcConfiguration;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.provider.jdbctemplate.JdbcTemplateLockProvider;
import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * ShedLock allows us to safely use Spring's @Scheduled annotation for scheduling background jobs
 * within a multi-instance environment.
 *
 * <p>Spring, by default, cannot handle scheduler synchronization over multiple instances. It
 * executes the jobs simultaneously on every node instead.
 *
 * <p>ShedLock makes sure that your scheduled tasks are executed at most once at the same time. If a
 * task is being executed on one node, it acquires a lock which prevents execution of the same task
 * from another node or thread. In the presence of an active lock, execution on other nodes is
 * skipped.
 *
 * <p>More information: https://github.com/lukas-krecan/ShedLock#jdbctemplate
 */
@Configuration
@EnableScheduling
@EnableSchedulerLock(defaultLockAtMostFor = "1h")
public class ShedlockConfiguration {

  private final JdbcTemplate jdbcTemplate;

  @Autowired
  public ShedlockConfiguration(DataRepoJdbcConfiguration jdbcConfiguration) {
    this.jdbcTemplate = new JdbcTemplate(jdbcConfiguration.getDataSource());
  }

  @Bean("shedlockLockProvider")
  public LockProvider lockProvider() {
    // By default, ShedLock looks for a table named `shedlock` in its data source to store its locks
    return new JdbcTemplateLockProvider(
        JdbcTemplateLockProvider.Configuration.builder()
            .withJdbcTemplate(jdbcTemplate)
            .usingDbTime()
            .build());
  }
}
