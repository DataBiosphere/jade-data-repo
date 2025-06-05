package bio.terra.service.dataset.flight.inheritsteward;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class InheritStewardAdjustMembersFlight extends Flight {
  public InheritStewardAdjustMembersFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    // Get the required DAOs and services to pass into the steps
    ApplicationContext appContext = (ApplicationContext) applicationContext;
    SnapshotService snapshotService = appContext.getBean(SnapshotService.class);
    IamService iamService = appContext.getBean(IamService.class);

    // Get the input parameters
    UUID datasetId = inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), UUID.class);
    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    boolean inheritSteward =
        inputParameters.get(JobMapKeys.INHERIT_STEWARD.getKeyName(), Boolean.class);

    addStep(
        new GetSnapshotIdsStep(snapshotService, iamService, userReq, datasetId, inheritSteward));
    addStep(new AdjustStewardMembersStep(userReq, iamService, inheritSteward));
  }
}
