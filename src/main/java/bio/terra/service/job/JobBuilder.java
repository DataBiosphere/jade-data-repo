package bio.terra.service.job;

import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.job.exception.InvalidJobParameterException;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import java.util.EnumSet;
import java.util.Set;

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
    JobMapKeys.DESCRIPTION.put(jobParameterMap, description);
    JobMapKeys.REQUEST.put(jobParameterMap, request);
    JobMapKeys.AUTH_USER_INFO.put(jobParameterMap, userReq);
    JobMapKeys.SUBJECT_ID.put(jobParameterMap, userReq.getSubjectId());
  }

  private static final Set<JobMapKeys> REQUIRED_KEYS =
      EnumSet.of(
          JobMapKeys.DESCRIPTION,
          JobMapKeys.REQUEST,
          JobMapKeys.AUTH_USER_INFO,
          JobMapKeys.SUBJECT_ID);

  // use addParameter method for optional parameter
  // returns the JobBuilder object to allow method chaining
  public JobBuilder addParameter(JobMapKeys key, Object val) {
    // check that keyName doesn't match one of the required parameter names
    // i.e. disallow overwriting one of the required parameters
    if (REQUIRED_KEYS.contains(key)) {
      throw new InvalidJobParameterException(
          "Required parameters can only be set by the constructor. (" + key + ")");
    }

    // note that this call overwrites a parameter if it already exists
    key.put(jobParameterMap, val);

    return this;
  }

  public JobBuilder addParameter(String key, Object val) {
    // note that this call overwrites a parameter if it already exists
    jobParameterMap.put(key, val);
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
