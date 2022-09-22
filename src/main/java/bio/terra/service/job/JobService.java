package bio.terra.service.job;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.configuration.StairwayJdbcConfiguration;
import bio.terra.app.logging.PerformanceLogger;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.kubernetes.KubeService;
import bio.terra.common.stairway.StairwayComponent;
import bio.terra.model.JobModel;
import bio.terra.model.SqlSortDirection;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.job.exception.InvalidResultStateException;
import bio.terra.service.job.exception.JobNotFoundException;
import bio.terra.service.job.exception.JobResponseException;
import bio.terra.service.job.exception.JobServiceShutdownException;
import bio.terra.service.job.exception.JobUnauthorizedException;
import bio.terra.service.upgrade.Migrate;
import bio.terra.stairway.ExceptionSerializer;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightFilter;
import bio.terra.stairway.FlightFilterOp;
import bio.terra.stairway.FlightFilterSortDirection;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.Stairway;
import bio.terra.stairway.exception.FlightNotFoundException;
import bio.terra.stairway.exception.StairwayException;
import bio.terra.stairway.exception.StairwayExecutionException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.commons.collections4.ListUtils;
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

  private final IamService samService;
  private final ApplicationConfiguration appConfig;
  private final StairwayComponent stairwayComponent;
  private final StairwayJdbcConfiguration stairwayJdbcConfiguration;
  private final KubeService kubeService;
  private final AtomicBoolean isRunning;
  private final Migrate migrate;
  private final ObjectMapper objectMapper;
  private final PerformanceLogger performanceLogger;
  private final ApplicationContext applicationContext;
  private Stairway stairway;

  @Autowired
  public JobService(
      IamService samService,
      ApplicationConfiguration appConfig,
      StairwayJdbcConfiguration stairwayJdbcConfiguration,
      StairwayComponent stairwayComponent,
      KubeService kubeService,
      ApplicationContext applicationContext,
      Migrate migrate,
      ObjectMapper objectMapper,
      PerformanceLogger performanceLogger)
      throws StairwayExecutionException {
    this.samService = samService;
    this.appConfig = appConfig;

    this.stairwayComponent = stairwayComponent;
    this.stairwayJdbcConfiguration = stairwayJdbcConfiguration;
    this.isRunning = new AtomicBoolean(true);
    this.migrate = migrate;
    this.applicationContext = applicationContext;
    this.objectMapper = objectMapper;
    this.performanceLogger = performanceLogger;
    this.kubeService = kubeService;
    initialize();
  }

  /**
   * This method is called from StartupInitializer as part of the sequence of migrating databases
   * and recovering any jobs; i.e., Stairway flights. It lives in this class so that JobService
   * encapsulates all Stairway interaction.
   */
  public void initialize() {
    migrate.migrateDatabase();

    // Initialize stairway - only do the stairway migration if we did the data repo migration
    ExceptionSerializer serializer = new StairwayExceptionSerializer(objectMapper);
    stairwayComponent.initialize(
        stairwayComponent
            .newStairwayOptionsBuilder()
            .dataSource(stairwayJdbcConfiguration.getDataSource())
            .context(applicationContext)
            .addHook(new StairwayLoggingHooks(performanceLogger))
            .exceptionSerializer(serializer));
    stairway = stairwayComponent.get();
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
    } catch (InterruptedException ex) {
      throw new JobServiceShutdownException("Job service interrupted", ex);
    }
  }

  // generate a new jobId
  private String createJobId() {
    // in the future, if we have multiple stairways, we may need to maintain a connection from job
    // id to flight id
    return stairway.createFlightId();
  }

  public JobModel mapFlightStateToJobModel(FlightState flightState) {
    FlightMap inputParameters = flightState.getInputParameters();
    String description = inputParameters.get(JobMapKeys.DESCRIPTION.getKeyName(), String.class);
    String submittedDate = flightState.getSubmitted().toString();
    JobModel.JobStatusEnum jobStatus = getJobStatus(flightState);

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

    return new JobModel()
        .id(flightState.getFlightId())
        .className(flightState.getClassName())
        .description(description)
        .jobStatus(jobStatus)
        .statusCode(statusCode.value())
        .submitted(submittedDate)
        .completed(completedDate);
  }

  private JobModel.JobStatusEnum getJobStatus(FlightState flightState) {
    FlightStatus flightStatus = flightState.getFlightStatus();
    switch (flightStatus) {
      case ERROR:
      case FATAL:
        return JobModel.JobStatusEnum.FAILED;
      case RUNNING:
        return JobModel.JobStatusEnum.RUNNING;
      case SUCCESS:
        if (getCompletionToFailureException(flightState).isPresent()) {
          return JobModel.JobStatusEnum.FAILED;
        }
        return JobModel.JobStatusEnum.SUCCEEDED;
    }
    return JobModel.JobStatusEnum.FAILED;
  }

  public List<JobModel> enumerateJobs(
      int offset,
      int limit,
      AuthenticatedUserRequest userReq,
      SqlSortDirection direction,
      String className,
      List<String> jobIds) {

    // if the user has access to all jobs, then fetch everything
    // otherwise, filter the jobs on the user
    FlightFilter filter = new FlightFilter();
    // Set the order to use to return values
    switch (direction) {
      case ASC:
        filter.submittedTimeSortDirection(FlightFilterSortDirection.ASC);
        break;
      case DESC:
        filter.submittedTimeSortDirection(FlightFilterSortDirection.DESC);
        break;
      default:
        throw new IllegalArgumentException(String.format("Unrecognized direction %s", direction));
    }

    // Filter on FQ class name if specified
    if (!StringUtils.isEmpty(className)) {
      filter.addFilterFlightClass(FlightFilterOp.EQUAL, className);
    }

    HashSet<String> jobIdSet = new HashSet<>(ListUtils.emptyIfNull(jobIds));
    List<FlightState> flightStateList;
    boolean canListAnyJob = checkUserCanListAnyJob(userReq);
    try {
      if (canListAnyJob) {
        flightStateList = stairway.getFlights(offset, limit, filter);
        if (!jobIdSet.isEmpty()) {
          flightStateList =
              flightStateList.stream()
                  .filter(flightState -> jobIdSet.contains(flightState.getFlightId()))
                  .collect(Collectors.toList());
        }
      } else {
        flightStateList = getAccessibleFlights(offset, limit, filter, userReq, jobIdSet);
      }
    } catch (InterruptedException ex) {
      throw new JobServiceShutdownException("Job service interrupted", ex);
    }

    return flightStateList.stream()
        .map(this::mapFlightStateToJobModel)
        .collect(Collectors.toList());
  }

  private List<FlightState> getAccessibleFlights(
      int offset,
      int limit,
      FlightFilter filter,
      AuthenticatedUserRequest userReq,
      HashSet<String> jobIdSet)
      throws InterruptedException {
    int start = 0;
    int end = offset + limit;
    HashMap<String, List<String>> roleMap = new HashMap<>();
    ArrayList<FlightState> flightStateList = new ArrayList<>();
    while (flightStateList.size() < offset + limit) {
      List<FlightState> filterResults = stairway.getFlights(start, end, filter);
      if (filterResults.size() == 0) {
        break;
      }
      filterResults.forEach(
          flightState -> {
            if (userLaunchedFlight(flightState, userReq) && includeFlight(flightState, jobIdSet)) {
              flightStateList.add(flightState);
            } else {
              FlightMap inputParameters = flightState.getInputParameters();
              IamResourceType resourceType =
                  inputParameters.get(
                      JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.class);
              String resourceId =
                  inputParameters.get(JobMapKeys.IAM_RESOURCE_ID.getKeyName(), String.class);
              IamAction action =
                  inputParameters.get(JobMapKeys.IAM_ACTION.getKeyName(), IamAction.class);
              if (resourceType != null & resourceId != null && action != null) {
                String key = String.format("%s-%s", resourceType, resourceId);
                List<String> userRoles = roleMap.get(key);
                if (userRoles == null) {
                  userRoles = samService.listActions(userReq, resourceType, resourceId);
                  roleMap.put(key, userRoles);
                }
                if (userRoles.contains(action.toString()) && includeFlight(flightState, jobIdSet)) {
                  flightStateList.add(flightState);
                }
              }
            }
          });
      start = end;
      end += offset + limit;
    }
    if (flightStateList.size() <= offset) {
      return new ArrayList<>();
    } else if (flightStateList.size() < offset + limit) {
      return flightStateList.subList(offset, flightStateList.size());
    }
    return flightStateList.subList(offset, offset + limit);
  }

  private boolean includeFlight(FlightState flightState, HashSet<String> jobIdSet) {
    return jobIdSet.isEmpty() || jobIdSet.contains(flightState.getFlightId());
  }

  private boolean userLaunchedFlight(FlightState flightState, AuthenticatedUserRequest userReq) {
    FlightMap inputParameters = flightState.getInputParameters();
    String flightSubjectId = inputParameters.get(JobMapKeys.SUBJECT_ID.getKeyName(), String.class);
    return StringUtils.equals(flightSubjectId, userReq.getSubjectId());
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
    } catch (InterruptedException ex) {
      throw new JobServiceShutdownException("Job service interrupted", ex);
    }
  }

  /**
   * There are four cases to handle here:
   *
   * <ol>
   *   <li>Flight is still running. Return a 202 (Accepted) response
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

    JobModel.JobStatusEnum jobStatus = getJobStatus(flightState);

    switch (jobStatus) {
      case FAILED:
        final Exception exceptionToThrow;

        if (flightState.getException().isPresent()) {
          exceptionToThrow = flightState.getException().get();
        } else if (getCompletionToFailureException(flightState).isPresent()) {
          exceptionToThrow = getCompletionToFailureException(flightState).get();
        } else {
          exceptionToThrow =
              new InvalidResultStateException("Failed operation with no exception reported");
        }
        if (exceptionToThrow instanceof RuntimeException) {
          throw (RuntimeException) exceptionToThrow;
        } else {
          throw new JobResponseException("wrap non-runtime exception", exceptionToThrow);
        }
      case SUCCEEDED:
        FlightMap resultMap = flightState.getResultMap().orElse(null);
        if (resultMap == null) {
          throw new InvalidResultStateException("No result map returned from flight");
        }
        HttpStatus statusCode =
            resultMap.get(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.class);
        if (statusCode == null) {
          statusCode = HttpStatus.OK;
        }
        return new JobResultWithStatus<T>()
            .statusCode(statusCode)
            .result(resultMap.get(JobMapKeys.RESPONSE.getKeyName(), resultClass));

      case RUNNING:
        return new JobResultWithStatus<T>().statusCode(HttpStatus.ACCEPTED);

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
        String resourceId =
            inputParameters.get(JobMapKeys.IAM_RESOURCE_ID.getKeyName(), String.class);
        IamResourceType resourceType =
            inputParameters.get(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.class);
        IamAction iamAction =
            inputParameters.get(JobMapKeys.IAM_ACTION.getKeyName(), IamAction.class);
        if (resourceId != null
            && resourceType != null
            && iamAction != null
            && !samService.isAuthorized(userReq, resourceType, resourceId, iamAction)) {
          throw new JobUnauthorizedException("Unauthorized");
        }
      }
    } catch (FlightNotFoundException ex) {
      throw new JobNotFoundException("Job not found", ex);
    } catch (InterruptedException ex) {
      throw new JobServiceShutdownException("Job service interrupted", ex);
    }
  }

  private Optional<Exception> getCompletionToFailureException(FlightState flightState) {
    return flightState
        .getResultMap()
        .filter(rm -> rm.containsKey(CommonMapKeys.COMPLETION_TO_FAILURE_EXCEPTION))
        .map(rm -> rm.get(CommonMapKeys.COMPLETION_TO_FAILURE_EXCEPTION, Exception.class));
  }
}
