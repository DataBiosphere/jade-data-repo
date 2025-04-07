package bio.terra.service.snapshot.flight.setpublic;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class SnapshotSetPublicFlight extends Flight {
  public SnapshotSetPublicFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);
    ApplicationContext appContext = (ApplicationContext) applicationContext;
    IamService iamService = appContext.getBean(IamService.class);

    UUID snapshotId =
        UUID.fromString(inputParameters.get(JobMapKeys.SNAPSHOT_ID.getKeyName(), String.class));
    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);
    boolean setPublic = inputParameters.get(JobMapKeys.SET_PUBLIC.getKeyName(), Boolean.class);

    addStep(new SetSnapshotPublicStep(snapshotId, setPublic, userReq, iamService));
  }
}
