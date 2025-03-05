package bio.terra.service.dataset.flight.inheritsteward;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class EnableInheritStewardFlight extends Flight {
  public EnableInheritStewardFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    // Get the required DAOs and services to pass into the steps
    ApplicationContext appContext = (ApplicationContext) applicationContext;
    DatasetDao datasetDao = appContext.getBean(DatasetDao.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    SnapshotService snapshotService = appContext.getBean(SnapshotService.class);
    IamService iamService = appContext.getBean(IamService.class);

    // Get the input parameters
    UUID datasetId = inputParameters.get(JobMapKeys.IAM_RESOURCE_ID.getKeyName(), UUID.class);
    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    addStep(new InheritStewardSetFlagStep(datasetDao, datasetId, true));
    addStep(new LockDatasetStep(datasetService, datasetId, false));
    addStep(
        new InheritStewardSetParentOnSnapshotsStep(
            snapshotService, iamService, datasetId, userReq));
  }
}
