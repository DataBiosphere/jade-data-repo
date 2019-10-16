package bio.terra.service.job;

import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.job.exception.InvalidJobParameterException;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;

public class JobBuilder {

    private JobService jobServiceRef;
    private Class<? extends Flight> flightClass;
    private FlightMap jobParameterMap;

    // constructor only takes required parameters
    public JobBuilder(String description, Class<? extends Flight> flightClass, Object request,
                      AuthenticatedUserRequest userReq, JobService jobServiceRef) {
        this.jobServiceRef = jobServiceRef;
        this.flightClass = flightClass;

        // initialize with required parameters
        this.jobParameterMap = new FlightMap();
        jobParameterMap.put(JobMapKeys.DESCRIPTION.getKeyName(), description);
        jobParameterMap.put(JobMapKeys.REQUEST.getKeyName(), request);
        jobParameterMap.put(JobMapKeys.AUTH_USER_INFO.getKeyName(), userReq);
    }

    // use addParameter method for optional parameter
    // returns the JobBuilder object to allow method chaining
    public JobBuilder addParameter(String keyName, Object val) {
        if (keyName == null) {
            throw new InvalidJobParameterException("Parameter name cannot be null.");
        }

        // check that keyName doesn't match one of the required parameter names
        // i.e. disallow overwriting one of the required parameters
        boolean isParameterRequired = keyName.equals(JobMapKeys.DESCRIPTION.getKeyName())
            || keyName.equals(JobMapKeys.FLIGHT_CLASS.getKeyName())
            || keyName.equals(JobMapKeys.REQUEST.getKeyName())
            || keyName.equals(JobMapKeys.AUTH_USER_INFO.getKeyName());
        if (isParameterRequired) {
            throw new InvalidJobParameterException(
                "Required parameters can only be set by the constructor. (" + keyName + ")");
        }

        // note that this call overwrites a parameter if it already exists
        jobParameterMap.put(keyName, val);

        return this;
    }

    // submits this job to stairway and returns the jobId immediately
    public String submit() {
        AuthenticatedUserRequest userReq = jobParameterMap.get(
            JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);
        return jobServiceRef.submit(flightClass, jobParameterMap, userReq);
    }

    // submits this job to stairway, waits until it finishes, then returns an instance of the result class
    public <T> T submitAndWait(Class<T> resultClass) {
        AuthenticatedUserRequest userReq = jobParameterMap.get(
            JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);
        return jobServiceRef.submitAndWait(flightClass, jobParameterMap, userReq, resultClass);
    }

}
