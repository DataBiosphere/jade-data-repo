package scripts.utils.tdrwrapper;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.model.JobModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WrapFuture<T> implements Future<T> {
  private static final Logger logger = LoggerFactory.getLogger(WrapFuture.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private static final long MAX_GET_WAIT_HOURS = 2;
  private static final int POLL_INTERVALS_SECONDS = 5;

  private final String jobId;
  private final RepositoryApi repositoryApi;
  private final Class<T> targetClass;

  public WrapFuture(String jobId, RepositoryApi repositoryApi, Class<T> targetClass) {
    this.jobId = jobId;
    this.repositoryApi = repositoryApi;
    this.targetClass = targetClass;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    JobModel job = DataRepoWrap.apiCallThrow(() -> repositoryApi.retrieveJob(jobId));
    return !job.getJobStatus().equals(JobModel.JobStatusEnum.RUNNING);
  }

  @Override
  public T get() throws InterruptedException, ExecutionException {
    try {
      return get(MAX_GET_WAIT_HOURS, TimeUnit.HOURS);
    } catch (TimeoutException ex) {
      throw new ExecutionException("Wrapper imposed timeout", ex);
    }
  }

  @Override
  public T get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    Instant endTime = Instant.now().plusSeconds(TimeUnit.SECONDS.convert(timeout, unit));
    int tryCount = 0;
    while (endTime.isAfter(Instant.now())) {
      JobModel job = DataRepoWrap.apiCallThrow(() -> repositoryApi.retrieveJob(jobId));
      if (job.getJobStatus().equals(JobModel.JobStatusEnum.RUNNING)) {
        tryCount++;
        logger.debug(
            "Sleep: try #{} until {} job {} {}", tryCount, endTime, jobId, job.getDescription());
        TimeUnit.SECONDS.sleep(POLL_INTERVALS_SECONDS);
      } else {
        break;
      }
    }

    Object result = DataRepoWrap.apiCallThrow(() -> repositoryApi.retrieveJobResult(jobId));
    return objectMapper.convertValue(result, targetClass);
  }
}
