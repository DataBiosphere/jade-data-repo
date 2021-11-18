package bio.terra.service.upgrade.flight;

import bio.terra.model.UpgradeModel;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

public class RowMetadataUpdateFlight extends Flight {
  public RowMetadataUpdateFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    ApplicationContext appContext = (ApplicationContext) applicationContext;
    // ProfileService profileService = (ProfileService) appContext.getBean("profileService");

    UpgradeModel request = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), UpgradeModel.class);

    addStep(new BackfillRowMetadataTablesStep(request));
  }
}
