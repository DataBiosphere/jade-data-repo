package bio.terra.tanagra.indexing;

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

  void run(boolean isDryRun, Indexer.Executors executors);

  void clean(boolean isDryRun, Indexer.Executors executors);

  JobStatus checkStatus(Indexer.Executors executors);

  default JobStatus execute(RunType runType, boolean isDryRun, Indexer.Executors executors) {
    LOGGER.info("Executing indexing job: {}, {}", runType, getName());
    JobStatus status = checkStatus(executors);
    LOGGER.info("Job status: {}", status);

    switch (runType) {
      case RUN -> {
        if (status != JobStatus.NOT_STARTED) {
          LOGGER.info("Skipping because job is either in progress or complete");
          return status;
        }
        run(isDryRun, executors);
        return checkStatus(executors);
      }
      case CLEAN -> {
        if (status == JobStatus.IN_PROGRESS) {
          LOGGER.info("Skipping because job is in progress");
          return status;
        }
        clean(isDryRun, executors);
        return checkStatus(executors);
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
