package bio.terra.datarepo.service.job;

import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;

public class JobTestShutdownFlight extends Flight {

  public JobTestShutdownFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    Integer flightWaitSeconds = inputParameters.get("flightWaitSeconds", Integer.class);

    addStep(new JobTestShutdownStep(flightWaitSeconds));
  }
}
