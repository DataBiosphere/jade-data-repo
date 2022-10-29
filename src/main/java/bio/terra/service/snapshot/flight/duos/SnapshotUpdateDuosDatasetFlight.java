package bio.terra.service.snapshot.flight.duos;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.duos.DuosDao;
import bio.terra.service.duos.DuosService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

/**
 * This flight links a snapshot to a DUOS dataset for the purposes of syncing its authorized users
 * as snapshot readers via a TDR-managed Firecloud group. It supports linking, unlinking, and
 * updating a snapshot's existing link (unlinking + linking).
 */
public class SnapshotUpdateDuosDatasetFlight extends Flight {

  public SnapshotUpdateDuosDatasetFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    ApplicationContext appContext = (ApplicationContext) applicationContext;
    DuosDao duosDao = appContext.getBean(DuosDao.class);
    DuosService duosService = appContext.getBean(DuosService.class);
    IamService iamService = appContext.getBean(IamService.class);
    SnapshotDao snapshotDao = appContext.getBean(SnapshotDao.class);

    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);
    UUID snapshotId = inputParameters.get(JobMapKeys.SNAPSHOT_ID.getKeyName(), UUID.class);
    String duosId = inputParameters.get(SnapshotDuosMapKeys.DUOS_ID, String.class);
    DuosFirecloudGroupModel firecloudGroupPrev =
        inputParameters.get(
            SnapshotDuosMapKeys.FIRECLOUD_GROUP_PREV, DuosFirecloudGroupModel.class);

    boolean linking = (duosId != null);
    boolean unlinking = (firecloudGroupPrev != null);

    if (unlinking) {
      addStep(
          new RemoveDuosFirecloudReaderStep(iamService, userReq, snapshotId, firecloudGroupPrev));
    }
    if (linking) {
      addStep(new RetrieveDuosFirecloudGroupStep(duosDao, duosId));
      // Create a
      addStep(
          new IfGroupDoesNotExistStep(
              new CreateDuosFirecloudGroupStep(duosService, iamService, duosId)));
      addStep(new IfGroupDoesNotExistStep(new RecordDuosFirecloudGroupStep(duosDao)));
      addStep(new AddDuosFirecloudReaderStep(iamService, userReq, snapshotId));
    }
    addStep(
        new UpdateSnapshotDuosFirecloudGroupIdStep(snapshotDao, snapshotId, firecloudGroupPrev));
  }
}
