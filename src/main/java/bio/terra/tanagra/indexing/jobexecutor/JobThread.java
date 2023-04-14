package bio.terra.tanagra.indexing.jobexecutor;

import bio.terra.tanagra.indexing.Indexer;
import bio.terra.tanagra.indexing.IndexingJob;
import java.util.concurrent.Callable;

/** Thread that runs a single indexing job and outputs an instance of the result class. */
public class JobThread implements Callable<JobResult> {
  private final IndexingJob indexingJob;
  private final boolean isDryRun;
  private final IndexingJob.RunType runType;
  private final String jobDescription;
  private final Indexer.Executors executors;

  public JobThread(
      IndexingJob indexingJob,
      boolean isDryRun,
      IndexingJob.RunType runType,
      String jobDescription,
      Indexer.Executors executors) {
    this.indexingJob = indexingJob;
    this.isDryRun = isDryRun;
    this.runType = runType;
    this.jobDescription = jobDescription;
    this.executors = executors;
  }

  @Override
  public JobResult call() {
    JobResult result = new JobResult(jobDescription, Thread.currentThread().getName());

    long startTime = System.nanoTime();
    try {
      IndexingJob.JobStatus status = indexingJob.execute(runType, isDryRun, executors);
      result.setJobStatus(status);
      result.setJobStatusAsExpected(
          IndexingJob.checkStatusAfterRunMatchesExpected(runType, isDryRun, status));
      result.setExceptionWasThrown(false);
    } catch (Throwable ex) {
      result.saveExceptionThrown(ex);
    }
    result.setElapsedTimeNS(System.nanoTime() - startTime);

    return result;
  }
}
