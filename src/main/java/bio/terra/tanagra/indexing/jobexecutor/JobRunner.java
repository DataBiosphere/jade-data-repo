package bio.terra.tanagra.indexing.jobexecutor;

import bio.terra.tanagra.exception.SystemException;
import bio.terra.tanagra.indexing.IndexingJob;
import bio.terra.tanagra.query.QueryExecutor;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;

public abstract class JobRunner {
  protected static final long MAX_TIME_PER_JOB_MIN = 60;
  protected static final long MAX_TIME_PER_JOB_DRY_RUN_MIN = 5;

  public static final String TERMINAL_ESCAPE_RESET = "\u001B[0m";
  public static final String TERMINAL_ANSI_PURPLE = "\u001B[35m";
  public static final String TERMINAL_ANSI_GREEN = "\u001b[32m";
  public static final String TERMINAL_ANSI_RED = "\u001b[31m";

  protected final List<SequencedJobSet> jobSets;
  protected final boolean isDryRun;
  protected final IndexingJob.RunType runType;
  protected final List<JobResult> jobResults;
  protected final QueryExecutor executor;

  public JobRunner(
      List<SequencedJobSet> jobSets,
      boolean isDryRun,
      IndexingJob.RunType runType,
      QueryExecutor executor) {
    this.jobSets = jobSets;
    this.isDryRun = isDryRun;
    this.runType = runType;
    this.jobResults = new ArrayList<>();
    this.executor = executor;
  }

  /** Name for display only. */
  protected abstract String getName();

  /** Run all job sets. */
  public abstract void runJobSets();

  /** Run a single job set. */
  protected abstract void runSingleJobSet(SequencedJobSet sequencedJobSet)
      throws InterruptedException, ExecutionException;

  /** Pretty print the job results to the terminal. */
  @SuppressWarnings("PMD.SystemPrintln")
  public void printJobResultSummary() {
    System.out.println(System.lineSeparator());
    System.out.println(
        TERMINAL_ANSI_PURPLE + "Indexing job summary (" + getName() + ")" + TERMINAL_ESCAPE_RESET);
    jobResults.stream()
        .sorted(Comparator.comparing(JobResult::getJobDescription, String.CASE_INSENSITIVE_ORDER))
        .forEach(JobResult::print);
  }

  public void throwIfAnyFailures() {
    jobResults.stream()
        .forEach(
            jobResult -> {
              if (jobResult.isFailure()) {
                throw new SystemException("There were job failures");
              }
            });
  }
}
