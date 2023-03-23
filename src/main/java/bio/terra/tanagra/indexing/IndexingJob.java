package bio.terra.tanagra.indexing;

import bio.terra.tanagra.query.QueryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface IndexingJob {
  Logger LOGGER = LoggerFactory.getLogger(IndexingJob.class);

  enum JobStatus {
    NOT_STARTED,
    IN_PROGRESS,
    COMPLETE
  }

  enum RunType {
    STATUS,
    CLEAN,
    RUN
  }

  String getName();

  void run(boolean isDryRun, QueryExecutor executor);

  void clean(boolean isDryRun, QueryExecutor executor);

  JobStatus checkStatus(QueryExecutor executor);

  default JobStatus execute(RunType runType, boolean isDryRun, QueryExecutor executor) {
    LOGGER.info("Executing indexing job: {}, {}", runType, getName());
    JobStatus status = checkStatus(executor);
    LOGGER.info("Job status: {}", status);

    switch (runType) {
      case RUN -> {
        if (status != JobStatus.NOT_STARTED) {
          LOGGER.info("Skipping because job is either in progress or complete");
          return status;
        }
        run(isDryRun, executor);
        return checkStatus(executor);
      }
      case CLEAN -> {
        if (status == JobStatus.IN_PROGRESS) {
          LOGGER.info("Skipping because job is in progress");
          return status;
        }
        clean(isDryRun, executor);
        return checkStatus(executor);
      }
      case STATUS -> {
        return status;
      }
      default -> throw new IllegalArgumentException("Unknown execution type: " + runType);
    }
  }

  /** Check if the job completed what it was supposed to. */
  static boolean checkStatusAfterRunMatchesExpected(
      RunType runType, boolean isDryRun, JobStatus status) {
    return isDryRun
        || RunType.STATUS == runType
        || (RunType.RUN == runType && JobStatus.COMPLETE == status)
        || (RunType.CLEAN == runType && JobStatus.NOT_STARTED == status);
  }
}
