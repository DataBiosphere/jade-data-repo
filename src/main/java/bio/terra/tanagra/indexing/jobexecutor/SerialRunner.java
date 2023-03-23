package bio.terra.tanagra.indexing.jobexecutor;

import bio.terra.tanagra.exception.SystemException;
import bio.terra.tanagra.indexing.IndexingJob;
import bio.terra.tanagra.query.QueryExecutor;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

/** Utility class that runs multiple job sets in serial. */
public final class SerialRunner extends JobRunner {
  public SerialRunner(
      List<SequencedJobSet> jobSets,
      boolean isDryRun,
      IndexingJob.RunType runType,
      QueryExecutor executor) {
    super(jobSets, isDryRun, runType, executor);
  }

  @Override
  protected String getName() {
    return "SERIAL";
  }

  /** Run all job sets serially. */
  @Override
  public void runJobSets() {
    jobSets.forEach(
        jobSet -> {
          try {
            runSingleJobSet(jobSet);
          } catch (InterruptedException | ExecutionException ex) {
            throw new SystemException("Job set execution failed", ex);
          }
        });
  }

  /** Run a single job set. Run the stages serially, and the jobs within each stage serially. */
  @Override
  protected void runSingleJobSet(SequencedJobSet sequencedJobSet)
      throws InterruptedException, ExecutionException {
    // Iterate through the job stages, running all jobs in each stage.
    Iterator<List<IndexingJob>> jobStagesIterator = sequencedJobSet.iterator();
    while (jobStagesIterator.hasNext()) {
      List<IndexingJob> jobsInStage = jobStagesIterator.next();
      for (IndexingJob job : jobsInStage) {
        JobResult jobResult = new JobThread(job, isDryRun, runType, job.getName(), executor).call();
        jobResult.setThreadTerminatedOnTime(true);
        jobResults.add(jobResult);
      }
    }
  }
}
