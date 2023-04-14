package bio.terra.tanagra.indexing.jobexecutor;

import static bio.terra.tanagra.indexing.jobexecutor.ParallelRunner.TERMINAL_ANSI_GREEN;
import static bio.terra.tanagra.indexing.jobexecutor.ParallelRunner.TERMINAL_ANSI_RED;
import static bio.terra.tanagra.indexing.jobexecutor.ParallelRunner.TERMINAL_ESCAPE_RESET;

import bio.terra.tanagra.indexing.IndexingJob;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Output of a thread that runs a single indexing job. */
public class JobResult {
  private static final Logger LOGGER = LoggerFactory.getLogger(JobResult.class);

  private final String jobDescription;
  private final String threadName;

  private IndexingJob.JobStatus jobStatus;
  private boolean threadTerminatedOnTime;
  private boolean jobStatusAsExpected;
  private long elapsedTimeNS;

  private boolean exceptionWasThrown;
  private String exceptionStackTrace;
  private String exceptionMessage;

  public JobResult(String jobDescription, String threadName) {
    this.jobDescription = jobDescription;
    this.threadName = threadName;

    this.threadTerminatedOnTime = false;
    this.jobStatusAsExpected = false;
    this.exceptionWasThrown = false;
    this.exceptionStackTrace = null;
    this.exceptionMessage = null;
  }

  @SuppressWarnings("PMD.SystemPrintln")
  public void print() {
    System.out.println(
        String.format(
            "%s %s",
            jobDescription,
            isFailure()
                ? (TERMINAL_ANSI_RED + "FAILED" + TERMINAL_ESCAPE_RESET)
                : (TERMINAL_ANSI_GREEN + "SUCCESS" + TERMINAL_ESCAPE_RESET)));
    System.out.println(String.format("   thread: %s", threadName));
    System.out.println(String.format("   job status: %s", jobStatus));
    System.out.println(String.format("   job status as expected: %s", jobStatusAsExpected));
    System.out.println(
        String.format(
            "   elapsed time (sec): %d",
            TimeUnit.MINUTES.convert(elapsedTimeNS, TimeUnit.NANOSECONDS)));
    System.out.println(String.format("   thread terminated on time: %s", threadTerminatedOnTime));
    System.out.println(String.format("   exception msg: %s", exceptionMessage));
    System.out.println(String.format("   exception stack trace: %s", exceptionStackTrace));
  }

  /**
   * Store the exception message and stack trace for the job results. Don't store the full {@link
   * Throwable} object, because that may not always be serializable. This class may be serialized to
   * disk as part of writing out the job results, so it needs to be a POJO.
   */
  public void saveExceptionThrown(Throwable exceptionThrown) {
    exceptionWasThrown = true;
    exceptionMessage =
        StringUtils.isBlank(exceptionMessage)
            ? exceptionThrown.getMessage()
            : String.format("%s%n%s", exceptionMessage, exceptionThrown.getMessage());

    StringWriter stackTraceStr = new StringWriter();
    exceptionThrown.printStackTrace(new PrintWriter(stackTraceStr));
    exceptionStackTrace = stackTraceStr.toString();

    LOGGER.error("Job thread threw error", exceptionThrown); // print the stack trace to the console
  }

  public boolean isFailure() {
    return exceptionWasThrown || !threadTerminatedOnTime || !jobStatusAsExpected;
  }

  public String getJobDescription() {
    return jobDescription;
  }

  public IndexingJob.JobStatus getJobStatus() {
    return jobStatus;
  }

  public void setJobStatus(IndexingJob.JobStatus jobStatus) {
    this.jobStatus = jobStatus;
  }

  public void setThreadTerminatedOnTime(boolean threadTerminatedOnTime) {
    this.threadTerminatedOnTime = threadTerminatedOnTime;
  }

  public void setJobStatusAsExpected(boolean jobStatusAsExpected) {
    this.jobStatusAsExpected = jobStatusAsExpected;
  }

  public void setElapsedTimeNS(long elapsedTimeNS) {
    this.elapsedTimeNS = elapsedTimeNS;
  }

  public void setExceptionWasThrown(boolean exceptionWasThrown) {
    this.exceptionWasThrown = exceptionWasThrown;
  }
}
