package bio.terra.service.job;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.configuration.StairwayJdbcConfiguration;
import bio.terra.app.logging.PerformanceLogger;
import bio.terra.common.kubernetes.KubeService;
import bio.terra.model.JobModel;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamAction;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamService;
import bio.terra.service.job.exception.InternalStairwayException;
import bio.terra.service.job.exception.InvalidResultStateException;
import bio.terra.service.job.exception.JobNotCompleteException;
import bio.terra.service.job.exception.JobNotFoundException;
import bio.terra.service.job.exception.JobResponseException;
import bio.terra.service.job.exception.JobServiceShutdownException;
import bio.terra.service.job.exception.JobServiceStartupException;
import bio.terra.service.job.exception.JobUnauthorizedException;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import bio.terra.service.upgrade.Migrate;
import bio.terra.service.upgrade.MigrateConfiguration;
import bio.terra.stairway.ExceptionSerializer;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightFilter;
import bio.terra.stairway.FlightFilterOp;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.Stairway;
import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.exception.FlightNotFoundException;
import bio.terra.stairway.exception.StairwayException;
import bio.terra.stairway.exception.StairwayExecutionException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
public class JobService {

  private static final Logger logger = LoggerFactory.getLogger(JobService.class);
  private static final int MIN_SHUTDOWN_TIMEOUT = 14;
  private static final int POD_LISTENER_SHUTDOWN_TIMEOUT = 2;
  private static final String API_POD_FILTER = "datarepo-api";

  private final Stairway stairway;
  private final IamService samService;
  private final ApplicationConfiguration appConfig;
  private final StairwayJdbcConfiguration stairwayJdbcConfiguration;
  private final MigrateConfiguration migrateConfiguration;
  private final KubeService kubeService;
  private final AtomicBoolean isRunning;
  private final Migrate migrate;

  @Autowired
  public JobService(
      IamService samService,
      ApplicationConfiguration appConfig,
      StairwayJdbcConfiguration stairwayJdbcConfiguration,
      MigrateConfiguration migrateConfiguration,
      GoogleResourceConfiguration googleResourceConfiguration,
      ApplicationContext applicationContext,
      Migrate migrate,
      ObjectMapper objectMapper,
      PerformanceLogger performanceLogger)
      throws StairwayExecutionException {
    this.samService = samService;
    this.appConfig = appConfig;

    this.stairwayJdbcConfiguration = stairwayJdbcConfiguration;
    this.migrateConfiguration = migrateConfiguration;
    this.isRunning = new AtomicBoolean(true);
    this.migrate = migrate;

    this.kubeService =
        new KubeService(appConfig.getPodName(), appConfig.isInKubernetes(), API_POD_FILTER);

    String projectId = googleResourceConfiguration.getProjectId();
    String stairwayClusterName = kubeService.getNamespace() + "-stairwaycluster";

    logger.info(
        "Creating Stairway: maxStairwayThreads: "
            + appConfig.getMaxStairwayThreads()
            + " in project: "
            + projectId);
    ExceptionSerializer serializer = new StairwayExceptionSerializer(objectMapper);
    stairway =
        Stairway.newBuilder()
            // for debugging stairway flights, set this true and the flight logs will be retained
            .keepFlightLog(true)
            .maxParallelFlights(appConfig.getMaxStairwayThreads())
            .exceptionSerializer(serializer)
            .applicationContext(applicationContext)
            .stairwayName(appConfig.getPodName())
            .stairwayHook(new StairwayLoggingHooks(performanceLogger))
            .stairwayClusterName(stairwayClusterName)
            .workQueueProjectId(projectId)
            .enableWorkQueue(appConfig.isInKubernetes())
            .build();
  }

  /**
   * This method is called from StartupInitializer as part of the sequence of migrating databases
   * and recovering any jobs; i.e., Stairway flights. It lives in this class so that JobService
   * encapsulates all Stairway interaction.
   */
  public void initialize() {
    try {
      List<String> recordedStairways;
      migrate.migrateDatabase();

      // Initialize stairway - only do the stairway migration if we did the data repo migration
      recordedStairways =
          stairway.initialize(
              stairwayJdbcConfiguration.getDataSource(),
              migrateConfiguration.getDropAllOnStart(),
              migrateConfiguration.getUpdateAllOnStart());

      // Order is important here. There are two concerns we need to handle:
      // 1. We need to avoid a window where a running pod could get onto the Stairway list, but not
      // be
      //    on the pod list. That is why we get the recorded list from Stairway *before* we read the
      // Kubernetes
      //    pod list.
      // 2. We want to clean up pods that fail, so we start the kubePodListener as early as possible
      // so we
      //    detect pods that get deleted. The Stairway recovery method called by kubePodListener
      // works once
      //    Stairway initialization is done, so it is safe to start the listener before we have
      // called
      //    Stairway recoveryAndStart
      kubeService.startPodListener(stairway);

      // Lookup all of the stairway instances we know about
      Set<String> existingStairways = kubeService.getPodSet();
      List<String> obsoleteStairways = new LinkedList<>();

      // Any instances that stairway knows about, but we cannot see are obsolete.
      for (String recordedStairway : recordedStairways) {
        if (!existingStairways.contains(recordedStairway)) {
          obsoleteStairways.add(recordedStairway);
        }
      }

      // Add our own pod name to the list of obsolete stairways. Sometimes Kubernetes will
      // restart the container without redeploying the pod. In that case we must ask
      // Stairway to recover the flights we were working on before being restarted.
      obsoleteStairways.add(kubeService.getPodName());

      // Recover and start stairway - step 3 of the stairway startup sequence
      stairway.recoverAndStart(obsoleteStairways);

    } catch (StairwayException stairwayEx) {
      throw new InternalStairwayException("Stairway initialization failed", stairwayEx);
    } catch (InterruptedException ex) {
      throw new JobServiceStartupException("Stairway startup process interrupted", ex);
    }
  }

  /** Stop accepting jobs and shutdown stairway */
  public boolean shutdown() throws InterruptedException {
    logger.info("JobService received shutdown request");
    boolean currentlyRunning = isRunning.getAndSet(false);
    if (!currentlyRunning) {
      logger.warn("Ignoring duplicate shutdown request");
      return true; // allow this to be success
    }

    // We enforce a minimum shutdown time. Otherwise, there is no point in trying the shutdown.
    // We allocate 3/4 of the time for graceful shutdown. Then call terminate for the rest of the
    // time.
    int shutdownTimeout = appConfig.getShutdownTimeoutSeconds();
    if (shutdownTimeout < MIN_SHUTDOWN_TIMEOUT) {
      logger.warn(
          "Shutdown timeout of "
              + shutdownTimeout
              + "is too small. Setting to "
              + MIN_SHUTDOWN_TIMEOUT);
      shutdownTimeout = MIN_SHUTDOWN_TIMEOUT;
    }

    kubeService.stopPodListener(TimeUnit.SECONDS, POD_LISTENER_SHUTDOWN_TIMEOUT);
    shutdownTimeout = shutdownTimeout - POD_LISTENER_SHUTDOWN_TIMEOUT;

    int gracefulTimeout = (shutdownTimeout * 3) / 4;
    int terminateTimeout = (shutdownTimeout - gracefulTimeout) - 2;

    logger.info("JobService request Stairway shutdown");
    boolean finishedShutdown = stairway.quietDown(gracefulTimeout, TimeUnit.SECONDS);
    if (!finishedShutdown) {
      logger.info("JobService request Stairway terminate");
      finishedShutdown = stairway.terminate(terminateTimeout, TimeUnit.SECONDS);
    }
    logger.info("JobService finished shutdown?: " + finishedShutdown);
    return finishedShutdown;
  }

  public int getActivePodCount() {
    return kubeService.getActivePodCount();
  }

  public static class JobResultWithStatus<T> {
    private T result;
    private HttpStatus statusCode;

    public T getResult() {
      return result;
    }

    public JobResultWithStatus<T> result(T result) {
      this.result = result;
      return this;
    }

    public HttpStatus getStatusCode() {
      return statusCode;
    }

    public JobResultWithStatus<T> statusCode(HttpStatus httpStatus) {
      this.statusCode = httpStatus;
      return this;
    }
  }

  // creates a new JobBuilder object and returns it.
  public JobBuilder newJob(
      String description,
      Class<? extends Flight> flightClass,
      Object request,
      AuthenticatedUserRequest userReq) {
    return new JobBuilder(description, flightClass, request, userReq, this);
  }

  // submit a new job to stairway
  // protected method intended to be called only from JobBuilder
  protected String submit(Class<? extends Flight> flightClass, FlightMap parameterMap) {
    if (isRunning.get()) {
      String jobId = createJobId();
      try {
        stairway.submit(jobId, flightClass, parameterMap);
      } catch (StairwayException stairwayEx) {
        throw new InternalStairwayException(stairwayEx);
      } catch (InterruptedException ex) {
        throw new JobServiceShutdownException("Job service interrupted", ex);
      }
      return jobId;
    }

    throw new JobServiceShutdownException("Job service is shut down. Cannot accept a flight");
  }

  // submit a new job to stairway, wait for it to finish, then return the result
  // protected method intended to be called only from JobBuilder
  protected <T> T submitAndWait(
      Class<? extends Flight> flightClass, FlightMap parameterMap, Class<T> resultClass) {
    String jobId = submit(flightClass, parameterMap);
    waitForJob(jobId);
    AuthenticatedUserRequest userReq =
        parameterMap.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    return retrieveJobResult(jobId, resultClass, userReq).getResult();
  }

  void waitForJob(String jobId) {
    try {
      stairway.waitForFlight(jobId, null, null);
    } catch (StairwayException stairwayEx) {
      throw new InternalStairwayException(stairwayEx);
    } catch (InterruptedException ex) {
      throw new JobServiceShutdownException("Job service interrupted", ex);
    }
  }

  // generate a new jobId
  private String createJobId() {
    // in the future, if we have multiple stairways, we may need to maintain a connection from job
    // id to flight id
    return stairway.createFlightId().toString();
  }

  public void releaseJob(String jobId, AuthenticatedUserRequest userReq) {
    try {
      if (userReq != null) {
        // currently, this check will be true for stewards only
        boolean canDeleteAnyJob =
            samService.isAuthorized(
                userReq,
                IamResourceType.DATAREPO,
                appConfig.getResourceId(),
                IamAction.DELETE_JOBS);

        // if the user has access to all jobs, no need to check for this one individually
        // otherwise, check that the user has access to this job before deleting
        if (!canDeleteAnyJob) {
          verifyUserAccess(jobId, userReq); // jobId=flightId
        }
      }
      stairway.deleteFlight(jobId, false);
    } catch (StairwayException stairwayEx) {
      throw new InternalStairwayException(stairwayEx);
    } catch (InterruptedException ex) {
      throw new JobServiceShutdownException("Job service interrupted", ex);
    }
  }

  public JobModel mapFlightStateToJobModel(FlightState flightState) {
    FlightMap inputParameters = flightState.getInputParameters();
    String description = inputParameters.get(JobMapKeys.DESCRIPTION.getKeyName(), String.class);
    FlightStatus flightStatus = flightState.getFlightStatus();
    String submittedDate = flightState.getSubmitted().toString();
    JobModel.JobStatusEnum jobStatus = getJobStatus(flightStatus);

    String completedDate = null;
    HttpStatus statusCode = HttpStatus.ACCEPTED;

    if (flightState.getCompleted().isPresent()) {
      FlightMap resultMap = getResultMap(flightState);
      // The STATUS_CODE return only needs to be used to return alternate success responses.
      // If it is not present, then we set it to the default OK status.
      statusCode = resultMap.get(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.class);
      if (statusCode == null) {
        statusCode = HttpStatus.OK;
      }

      completedDate = flightState.getCompleted().get().toString();
    }

    JobModel jobModel =
        new JobModel()
            .id(flightState.getFlightId())
            .description(description)
            .jobStatus(jobStatus)
            .statusCode(statusCode.value())
            .submitted(submittedDate)
            .completed(completedDate);

    return jobModel;
  }

  private JobModel.JobStatusEnum getJobStatus(FlightStatus flightStatus) {
    switch (flightStatus) {
      case ERROR:
        return JobModel.JobStatusEnum.FAILED;
      case FATAL:
        return JobModel.JobStatusEnum.FAILED;
      case RUNNING:
        return JobModel.JobStatusEnum.RUNNING;
      case SUCCESS:
        return JobModel.JobStatusEnum.SUCCEEDED;
    }
    return JobModel.JobStatusEnum.FAILED;
  }

  public List<JobModel> enumerateJobs(int offset, int limit, AuthenticatedUserRequest userReq) {

    boolean canListAnyJob = checkUserCanListAnyJob(userReq);

    // if the user has access to all jobs, then fetch everything
    // otherwise, filter the jobs on the user
    List<FlightState> flightStateList;
    try {
      FlightFilter filter = new FlightFilter();

      if (!canListAnyJob) {
        filter.addFilterInputParameter(
            JobMapKeys.SUBJECT_ID.getKeyName(), FlightFilterOp.EQUAL, userReq.getSubjectId());
      }

      flightStateList = stairway.getFlights(offset, limit, filter);
    } catch (StairwayException stairwayEx) {
      throw new InternalStairwayException(stairwayEx);
    } catch (InterruptedException ex) {
      throw new JobServiceShutdownException("Job service interrupted", ex);
    }

    List<JobModel> jobModelList = new ArrayList<>();
    for (FlightState flightState : flightStateList) {
      JobModel jobModel = mapFlightStateToJobModel(flightState);
      jobModelList.add(jobModel);
    }
    return jobModelList;
  }

  public JobModel retrieveJob(String jobId, AuthenticatedUserRequest userReq) {
    boolean canListAnyJob = checkUserCanListAnyJob(userReq);

    try {
      // if the user has access to all jobs, then fetch the requested one
      // otherwise, check that the user has access to it first
      if (!canListAnyJob) {
        verifyUserAccess(jobId, userReq); // jobId=flightId
      }
      FlightState flightState = stairway.getFlightState(jobId);
      return mapFlightStateToJobModel(flightState);
    } catch (StairwayException stairwayEx) {
      throw new InternalStairwayException(stairwayEx);
    } catch (InterruptedException ex) {
      throw new JobServiceShutdownException("Job service interrupted", ex);
    }
  }

  /**
   * There are four cases to handle here:
   *
   * <ol>
   *   <li>Flight is still running. Throw an JobNotComplete exception
   *   <li>Successful flight: extract the resultMap RESPONSE as the target class. If a
   *       statusContainer is present, we try to retrieve the STATUS_CODE from the resultMap and
   *       store it in the container. That allows flight steps used in async REST API endpoints to
   *       set alternate success status codes. The status code defaults to OK, if it is not set in
   *       the resultMap.
   *   <li>Failed flight: if there is an exception, throw it. Note that we can only throw
   *       RuntimeExceptions to be handled by the global exception handler. Non-runtime exceptions
   *       require throw clauses on the controller methods; those are not present in the
   *       swagger-generated code, so it introduces a mismatch. Instead, in this code if the caught
   *       exception is not a runtime exception, then we throw JobResponseException passing in the
   *       Throwable to the exception. In the global exception handler, we retrieve the Throwable
   *       and use the error text from that in the error model
   *   <li>Failed flight: no exception present. We throw InvalidResultState exception
   * </ol>
   *
   * @param jobId to process
   * @return object of the result class pulled from the result map
   */
  public <T> JobResultWithStatus<T> retrieveJobResult(
      String jobId, Class<T> resultClass, AuthenticatedUserRequest userReq) {
    boolean canListAnyJob = checkUserCanListAnyJob(userReq);

    try {
      // if the user has access to all jobs, then fetch the requested result
      // otherwise, check that the user has access to it first
      if (!canListAnyJob) {
        verifyUserAccess(jobId, userReq); // jobId=flightId
      }
      return retrieveJobResultWorker(jobId, resultClass);
    } catch (StairwayException stairwayEx) {
      throw new InternalStairwayException(stairwayEx);
    } catch (InterruptedException ex) {
      throw new JobServiceShutdownException("Job service interrupted", ex);
    }
  }

  /**
   * In general, we keep Stairway encapsulated in the JobService. KubeService also needs access, so
   * we provide this accessor.
   *
   * @return stairway instance
   */
  public Stairway getStairway() {
    return stairway;
  }

  private <T> JobResultWithStatus<T> retrieveJobResultWorker(String jobId, Class<T> resultClass)
      throws StairwayException, InterruptedException {

    FlightState flightState = stairway.getFlightState(jobId);
    FlightMap resultMap = flightState.getResultMap().orElse(null);
    if (resultMap == null) {
      throw new InvalidResultStateException("No result map returned from flight");
    }

    switch (flightState.getFlightStatus()) {
      case FATAL:
      case ERROR:
        if (flightState.getException().isPresent()) {
          Exception exception = flightState.getException().get();
          if (exception instanceof RuntimeException) {
            throw (RuntimeException) exception;
          } else {
            throw new JobResponseException("wrap non-runtime exception", exception);
          }
        }
        throw new InvalidResultStateException("Failed operation with no exception reported");

      case SUCCESS:
        HttpStatus statusCode =
            resultMap.get(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.class);
        if (statusCode == null) {
          statusCode = HttpStatus.OK;
        }
        return new JobResultWithStatus<T>()
            .statusCode(statusCode)
            .result(resultMap.get(JobMapKeys.RESPONSE.getKeyName(), resultClass));

      case RUNNING:
        throw new JobNotCompleteException(
            "Attempt to retrieve job result before job is complete; job id: "
                + flightState.getFlightId());

      default:
        throw new InvalidResultStateException("Impossible case reached");
    }
  }

  private FlightMap getResultMap(FlightState flightState) {
    FlightMap resultMap = flightState.getResultMap().orElse(null);
    if (resultMap == null) {
      throw new InvalidResultStateException("No result map returned from flight");
    }
    return resultMap;
  }

  private boolean checkUserCanListAnyJob(AuthenticatedUserRequest userReq) {
    // TODO: investigate stubbing out SAM service in unit tests, then throw an exception here
    // instead,
    // or at least default to something other than steward-level access.
    // this check is currently intended for handling unit tests that want to bypass a SAM check,
    // not as an error checking mechanism in production.
    if (userReq == null) {
      return true;
    }

    // currently, this check will be true for stewards only
    return samService.isAuthorized(
        userReq, IamResourceType.DATAREPO, appConfig.getResourceId(), IamAction.LIST_JOBS);
  }

  private void verifyUserAccess(String jobId, AuthenticatedUserRequest userReq) {
    try {
      FlightState flightState = stairway.getFlightState(jobId);
      FlightMap inputParameters = flightState.getInputParameters();
      String flightSubjectId =
          inputParameters.get(JobMapKeys.SUBJECT_ID.getKeyName(), String.class);
      if (!StringUtils.equals(flightSubjectId, userReq.getSubjectId())) {
        throw new JobUnauthorizedException("Unauthorized");
      }
    } catch (DatabaseOperationException ex) {
      throw new InternalStairwayException("Stairway exception looking up the job", ex);
    } catch (FlightNotFoundException ex) {
      throw new JobNotFoundException("Job not found", ex);
    } catch (InterruptedException ex) {
      throw new JobServiceShutdownException("Job service interrupted", ex);
    }
  }
}
