package bio.terra.service;

import bio.terra.configuration.ApplicationConfiguration;
import bio.terra.controller.AuthenticatedUserRequest;
import bio.terra.model.JobModel;
import bio.terra.service.exception.InvalidResultStateException;
import bio.terra.service.exception.JobNotCompleteException;
import bio.terra.service.exception.JobResponseException;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.Stairway;
import bio.terra.stairway.UserRequestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class JobService {

    private static final Logger logger = LoggerFactory.getLogger(JobService.class);
    private final Stairway stairway;
    private final SamClientService samService;
    private final ApplicationConfiguration appConfig;

    @Autowired
    public JobService(Stairway stairway, SamClientService samService, ApplicationConfiguration appConfig) {
        this.stairway = stairway;
        this.samService = samService;
        this.appConfig = appConfig;
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

    private String createJobId() {
        // in the future, if we have multiple stairways, we may need to maintain a connection from job id to flight id
        return stairway.createFlightId().toString();
    }

    public String submit(
        String description,
        Class<? extends Flight> flightClass,
        Object request,
        Map<String, String> params,
        AuthenticatedUserRequest userReq) {
        return submitToStairway(description, flightClass, request, params, userReq);
    }

    public <T> T submitAndWait(
        String description,
        Class<? extends Flight> flightClass,
        Object request,
        Map<String, String> params,
        AuthenticatedUserRequest userReq,
        Class<T> resultClass) {
        String jobId = submitToStairway(description, flightClass, request, params, userReq);
        stairway.waitForFlight(jobId);
        return retrieveJobResult(jobId, resultClass, userReq).getResult();
    }

    private String submitToStairway(
        String description,
        Class<? extends Flight> flightClass,
        Object request,
        Map<String, String> params,
        AuthenticatedUserRequest userReq) {
        String jobId = createJobId();
        stairway.submit(
            jobId,
            flightClass,
            buildFlightMap(description, request, params, userReq),
            buildUserRequestInfo(userReq));
        return jobId;
    }


    // TODO: fix this
    // There are at least two problems here. First, Stairway uses a map of <String, Object>, but the parameter
    // map only takes strings. Second, instead of putting the param elements into the inputParameters FlightMap
    // a level of indirection was introduced. That required that code be changed in every flight to unpack
    // those parameters to get out the actual parameters.
    //
    // I think a better approach would be to make a job parameter builder class that could construct with the
    // required params (class, description, request) and then have the job creator add additional params.
    // Then that class would be passed into submit along with the auth info.
    private FlightMap buildFlightMap(
        String description, Object request, Map<String, String> params, AuthenticatedUserRequest userReq) {
        FlightMap inputs = new FlightMap();
        inputs.put(JobMapKeys.DESCRIPTION.getKeyName(), description);
        inputs.put(JobMapKeys.REQUEST.getKeyName(), request);
        inputs.put(JobMapKeys.PATH_PARAMETERS.getKeyName(), params);
        inputs.put(JobMapKeys.AUTH_USER_INFO.getKeyName(), userReq);
        return inputs;
    }

    private UserRequestInfo buildUserRequestInfo(AuthenticatedUserRequest userReq) {
        return new UserRequestInfo()
            .name(userReq.getEmail())
            .subjectId(userReq.getSubjectId())
            .requestId(userReq.getReqId());
    }

    public void releaseJob(String jobId, AuthenticatedUserRequest userReq) {
        if (userReq!=null) {
            // currently, this check will be true for stewards only
            boolean canDeleteAnyJob = samService.isAuthorized(
                userReq,
                SamClientService.ResourceType.DATAREPO,
                appConfig.getResourceId(),
                SamClientService.DataRepoAction.DELETE_JOBS);

            // if the user has access to all jobs, no need to check for this one individually
            // otherwise, check that the user has access to this job before deleting
            if (!canDeleteAnyJob) {
                // throws exception if no access
                stairway.verifyUserAccess(jobId, buildUserRequestInfo(userReq)); // jobId=flightId
            }
        }
        stairway.deleteFlight(jobId);
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

        JobModel jobModel = new JobModel()
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

    public List<JobModel> enumerateJobs(
        int offset, int limit, AuthenticatedUserRequest userReq) {
        boolean canListAnyJob = true;
        if (userReq!=null) {
            // currently, this check will be true for stewards only
            canListAnyJob = samService.isAuthorized(
                userReq,
                SamClientService.ResourceType.DATAREPO,
                appConfig.getResourceId(),
                SamClientService.DataRepoAction.LIST_JOBS);
        }

        // if the user has access to all jobs, then fetch everything
        // otherwise, filter the jobs on the user
        List<FlightState> flightStateList;
        if (canListAnyJob) {
            flightStateList = stairway.getFlights(offset, limit);
        } else {
            flightStateList = stairway.getFlightsForUser(offset, limit, buildUserRequestInfo(userReq));
        }

        List<JobModel> jobModelList = new ArrayList<>();
        for (FlightState flightState : flightStateList) {
            JobModel jobModel = mapFlightStateToJobModel(flightState);
            jobModelList.add(jobModel);
        }
        return jobModelList;
    }

    public JobModel retrieveJob(String jobId, AuthenticatedUserRequest userReq) {
        boolean canListAnyJob = true;
        if (userReq!=null) {
            // currently, this check will be true for stewards only
            canListAnyJob = samService.isAuthorized(
                userReq,
                SamClientService.ResourceType.DATAREPO,
                appConfig.getResourceId(),
                SamClientService.DataRepoAction.LIST_JOBS);
        }

        // if the user has access to all jobs, then fetch the requested one
        // otherwise, check that the user has access to it first
        if (!canListAnyJob) {
            stairway.verifyUserAccess(jobId, buildUserRequestInfo(userReq)); // jobId=flightId
        }
        FlightState flightState = stairway.getFlightState(jobId);
        return mapFlightStateToJobModel(flightState);
    }

    /**
     * There are four cases to handle here:
     * <ol>
     *     <li> Flight is still running. Throw an JobNotComplete exception</li>
     *     <li> Successful flight: extract the resultMap RESPONSE as the target class. If a statusContainer
     *     is present, we try to retrieve the STATUS_CODE from the resultMap and store it in the container.
     *     That allows flight steps used in async REST API endpoints to set alternate success status codes.
     *     The status code defaults to OK, if it is not set in the resultMap.</li>
     *     <li> Failed flight: if there is an exception, throw it. Note that we can only throw RuntimeExceptions
     *     to be handled by the global exception handler. Non-runtime exceptions require throw clauses on
     *     the controller methods; those are not present in the swagger-generated code, so it introduces a
     *     mismatch. Instead, in this code if the caught exception is not a runtime exception, then we
     *     throw JobResponseException passing in the Throwable to the exception. In the global exception
     *     handler, we retrieve the Throwable and use the error text from that in the error model</li>
     *     <li> Failed flight: no exception present. We throw InvalidResultState exception</li>
     * </ol>
     * @param jobId to process
     * @return object of the result class pulled from the result map
     */
    public <T> JobResultWithStatus<T> retrieveJobResult(
        String jobId,
        Class<T> resultClass,
        AuthenticatedUserRequest userReq) {
        boolean canListAnyJob = true;
        if (userReq!=null) {
            // currently, this check will be true for stewards only
            canListAnyJob = samService.isAuthorized(
                userReq,
                SamClientService.ResourceType.DATAREPO,
                appConfig.getResourceId(),
                SamClientService.DataRepoAction.LIST_JOBS);
        }

        // if the user has access to all jobs, then fetch the requested result
        // otherwise, check that the user has access to it first
        if (!canListAnyJob) {
            stairway.verifyUserAccess(jobId, buildUserRequestInfo(userReq)); // jobId=flightId
        }
        return retrieveJobResultWorker(jobId, resultClass);
    }

    private <T> JobResultWithStatus<T> retrieveJobResultWorker(
        String jobId,
        Class<T> resultClass) {
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
                        throw (RuntimeException)exception;
                    } else {
                        throw new JobResponseException("wrap non-runtime exception", exception);
                    }
                }
                throw new InvalidResultStateException("Failed operation with no exception reported");

            case SUCCESS:
                HttpStatus statusCode = resultMap.get(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.class);
                if (statusCode == null) {
                    statusCode = HttpStatus.OK;
                }
                return  new JobResultWithStatus<T>()
                    .statusCode(statusCode)
                    .result(resultMap.get(JobMapKeys.RESPONSE.getKeyName(), resultClass));
            case RUNNING:
                throw new JobNotCompleteException("Attempt to retrieve job result before job is complete; job id: "
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

}
