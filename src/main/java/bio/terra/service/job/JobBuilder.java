package bio.terra.service.job;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.job.exception.InvalidJobParameterException;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;

public class JobBuilder {

  private JobService jobServiceRef;
  private Class<? extends Flight> flightClass;
  private FlightMap jobParameterMap;

  // constructor only takes required parameters
  public JobBuilder(
      String description,
      Class<? extends Flight> flightClass,
      Object request,
      AuthenticatedUserRequest userReq,
      JobService jobServiceRef) {
    this.jobServiceRef = jobServiceRef;
    this.flightClass = flightClass;

    // initialize with required parameters
    this.jobParameterMap = new FlightMap();
    jobParameterMap.put(JobMapKeys.DESCRIPTION.getKeyName(), description);
    jobParameterMap.put(JobMapKeys.REQUEST.getKeyName(), request);
    jobParameterMap.put(JobMapKeys.AUTH_USER_INFO.getKeyName(), userReq);
    jobParameterMap.put(JobMapKeys.SUBJECT_ID.getKeyName(), userReq.getSubjectId());
  }

  // use addParameter method for optional parameter
  // returns the JobBuilder object to allow method chaining
  public JobBuilder addParameter(String keyName, Object val) {
    if (keyName == null) {
      throw new InvalidJobParameterException("Parameter name cannot be null.");
    }

    // check that keyName doesn't match one of the required parameter names
    // i.e. disallow overwriting one of the required parameters
    if (keyName.equals(JobMapKeys.DESCRIPTION.getKeyName())
        || keyName.equals(JobMapKeys.REQUEST.getKeyName())
        || keyName.equals(JobMapKeys.AUTH_USER_INFO.getKeyName())
        || keyName.equals((JobMapKeys.SUBJECT_ID.getKeyName()))) {
      throw new InvalidJobParameterException(
          "Required parameters can only be set by the constructor. (" + keyName + ")");
    }

    // note that this call overwrites a parameter if it already exists
    jobParameterMap.put(keyName, val);

    return this;
  }

  // submits this job to stairway and returns the jobId immediately
  public String submit() {
    return jobServiceRef.submit(flightClass, jobParameterMap);
  }

  // submits this job to stairway, waits until it finishes, then returns an instance of the result
  // class
  public <T> T submitAndWait(Class<T> resultClass) {
    return jobServiceRef.submitAndWait(flightClass, jobParameterMap, resultClass);
  }
}
