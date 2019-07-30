package bio.terra.service;

import bio.terra.controller.AuthenticatedUserRequest;
import bio.terra.controller.RepositoryApiController;
import bio.terra.model.JobModel;
import bio.terra.service.exception.InvalidResultStateException;
import bio.terra.service.exception.JobNotCompleteException;
import bio.terra.service.exception.JobResponseException;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.Stairway;
import bio.terra.stairway.exception.FlightException;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.model.ResourceAndAccessPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class JobService {

    private static final Logger logger = LoggerFactory.getLogger(JobService.class);
    private final Stairway stairway;
    private SamClientService samClient;

    @Autowired
    public JobService(Stairway stairway, SamClientService samClient) {
        this.stairway = stairway;
        this.samClient = samClient;
    }

    private String createJobId() {
        // in the future, if we have multiple stairways, we may need to maintain a connection from job id to flight id
        return stairway.createFlightId().toString();
    }

    public String submit(
        String description, Class<? extends Flight> flightClass, Object request, AuthenticatedUserRequest userInfo) {
        String jobId = createJobId();
        try {
            samClient.createJobResource(userInfo, jobId);
        } catch (ApiException ex) {
            logger.warn("couldn't create sam resource for Job", ex);
            throw new FlightException("Couldn't create sam resource for job", ex);
        }
        submitToStairway(jobId, flightClass, new FlightMap(description, request, userInfo));
        return jobId;
    }

    public <T> T submitAndWait(
        String description,
        Class<? extends Flight> flightClass,
        Object request,
        AuthenticatedUserRequest userInfo,
        Class<T> resultClass) {
        String jobId = createJobId();
        submitToStairway(jobId, flightClass, new FlightMap(description, request, userInfo));
        stairway.waitForFlight(jobId);
        return retrieveJobResult(jobId, resultClass, null);
    }

    private void submitToStairway(
        String jobId, Class<? extends Flight> flightClass, FlightMap inputParams) {
        stairway.submit(jobId, flightClass, inputParams);
    }

    public void releaseJob(String jobId, AuthenticatedUserRequest userInfo) throws ApiException {
        samClient.deleteJobResource(userInfo, jobId);
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

    public List<JobModel> enumerateJobs(int offset, int limit, List<ResourceAndAccessPolicy> authorizedJobs) {
        if (authorizedJobs.isEmpty()) {
            return Collections.EMPTY_LIST;
        }
        List<String> resourceIds = authorizedJobs
            .stream()
            .map(resource -> resource.getResourceId())
            .collect(Collectors.toList());
        List<FlightState> flightStateList = stairway.getFlights(offset, limit, resourceIds); // TODO
        List<JobModel> jobModelList = new ArrayList<>();
        for (FlightState flightState : flightStateList) {
            JobModel jobModel = mapFlightStateToJobModel(flightState);
            jobModelList.add(jobModel);
        }
        return jobModelList;
    }

    public JobModel retrieveJob(
        String jobId) {
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
    public <T> T retrieveJobResult(
        String jobId, Class<T> resultClass, RepositoryApiController.HttpStatusContainer statContainer) {
        return retrieveJobResultWorker(jobId, resultClass, statContainer);
    }

    private <T> T retrieveJobResultWorker(
        String jobId,
        Class<T> resultClass,
        RepositoryApiController.HttpStatusContainer statusContainer) {
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
                if (statusContainer != null) {
                    HttpStatus statusCode = resultMap.get(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.class);
                    if (statusCode == null) {
                        statusCode = HttpStatus.OK;
                    }
                    statusContainer.setStatusCode(statusCode);
                }
                return resultMap.get(JobMapKeys.RESPONSE.getKeyName(), resultClass);

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
