package bio.terra.tanagra.indexing.jobexecutor;

import bio.terra.tanagra.exception.SystemException;
import bio.terra.tanagra.indexing.Indexer;
import bio.terra.tanagra.indexing.IndexingJob;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class that runs multiple job sets in parallel. */
public final class ParallelRunner extends JobRunner {
  private static final Logger LOGGER = LoggerFactory.getLogger(ParallelRunner.class);
  private static final long MAX_TIME_FOR_SHUTDOWN_SEC = 60;

  public ParallelRunner(
      List<SequencedJobSet> jobSets,
      boolean isDryRun,
      IndexingJob.RunType runType,
      Indexer.Executors executors) {
    super(jobSets, isDryRun, runType, executors);
  }

  @Override
  protected String getName() {
    return "PARALLEL";
  }

  /** Run all job sets in parallel. */
  @Override
  public void runJobSets() {
    // Create a thread pool to run the job set.
    ThreadPoolExecutor threadPool =
        (ThreadPoolExecutor) Executors.newFixedThreadPool(jobSets.size());
    LOGGER.info("Running job sets in parallel: {} sets", jobSets.size());

    // Kick off each job set in a separate thread.
    jobSets.forEach(
        jobSet ->
            threadPool.submit(
                () -> {
                  try {
                    runSingleJobSet(jobSet);
                  } catch (InterruptedException | ExecutionException ex) {
                    throw new SystemException("Job set execution failed", ex);
                  }
                }));

    try {
      LOGGER.info("Waiting for job set to to finish");
      shutdownThreadPool(threadPool);
    } catch (InterruptedException intEx) {
      LOGGER.error("Error running jobs in parallel. Try running in serial.");
      throw new SystemException("Error running jobs in parallel", intEx);
    }
  }

  /** Run a single job set. Run the stages serially, and the jobs within each stage in parallel. */
  @Override
  protected void runSingleJobSet(SequencedJobSet sequencedJobSet)
      throws InterruptedException, ExecutionException {
    // Iterate through the job stages, running all jobs in each stage.
    Iterator<List<IndexingJob>> jobStagesIterator = sequencedJobSet.iterator();
    while (jobStagesIterator.hasNext()) {
      List<IndexingJob> jobsInStage = jobStagesIterator.next();

      // Create a thread pool for each stage, one thread per job.
      ThreadPoolExecutor threadPool =
          (ThreadPoolExecutor) Executors.newFixedThreadPool(jobsInStage.size());
      LOGGER.info("New job stage: {} jobs", jobsInStage.size());

      // Kick off one thread per job.
      List<Future<JobResult>> jobFutures = new ArrayList<>();
      for (IndexingJob job : jobsInStage) {
        LOGGER.info("Kicking off thread for job set: {}", job.getName());
        Future<JobResult> jobFuture =
            threadPool.submit(new JobThread(job, isDryRun, runType, job.getName(), executors));
        jobFutures.add(jobFuture);
      }

      // Wait for all jobs in this stage to finish, before moving to the next stage.
      LOGGER.info("Waiting for jobs in stage to finish");
      shutdownThreadPool(threadPool);

      // Compile the results.
      for (Future<JobResult> jobFuture : jobFutures) {
        JobResult jobResult = jobFuture.get();
        jobResult.setThreadTerminatedOnTime(true);
        jobResults.add(jobResult);
      }

      LOGGER.info("Stage complete");
    }
  }

  /**
   * Tell a thread pool to stop accepting new jobs, wait for the existing jobs to finish. If the
   * jobs time out, then interrupt the threads and force them to terminate.
   */
  private void shutdownThreadPool(ThreadPoolExecutor threadPool) throws InterruptedException {
    // Wait for all threads to finish.
    threadPool.shutdown();
    boolean terminatedByItself =
        threadPool.awaitTermination(
            isDryRun ? MAX_TIME_PER_JOB_DRY_RUN_MIN : MAX_TIME_PER_JOB_MIN, TimeUnit.MINUTES);

    // If the threads didn't finish in the expected time, then send them interrupts.
    if (!terminatedByItself) {
      threadPool.shutdownNow();
    }
    if (!threadPool.awaitTermination(MAX_TIME_FOR_SHUTDOWN_SEC, TimeUnit.SECONDS)) {
      LOGGER.error("All threads in the pool failed to terminate");
    }
  }
}
