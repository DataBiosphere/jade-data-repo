package bio.terra.service.job;

import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.job.exception.InvalidJobParameterException;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import java.util.EnumSet;
import java.util.Set;

public class JobBuilder {

  private final JobService jobServiceRef;
  private final Class<? extends Flight> flightClass;
  private final FlightMap jobParameterMap;

  private static final Set<JobMapKeys> REQUIRED_KEYS =
      EnumSet.of(
          JobMapKeys.DESCRIPTION,
          JobMapKeys.REQUEST,
          JobMapKeys.AUTH_USER_INFO,
          JobMapKeys.SUBJECT_ID);

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
    jobParameterMap = new FlightMap();
    JobMapKeys.DESCRIPTION.put(jobParameterMap, description);
    // To support JobBuilder's constructor, some code passes `null` for `REQUEST` keys. Instead
    // the code could provide a constructor override that omits the unneeded parameter.
    if (request != null) {
      JobMapKeys.REQUEST.put(jobParameterMap, request);
    }
    JobMapKeys.AUTH_USER_INFO.put(jobParameterMap, userReq);
    JobMapKeys.SUBJECT_ID.put(jobParameterMap, userReq.getSubjectId());
  }

  /**
   * Call this method to add an optional parameter. The JobBuilder object is returned to allow
   * method chaining.
   *
   * @param key the parameter key
   * @param val the parameter value
   * @return the current object
   */
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
}
