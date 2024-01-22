package bio.terra.service.snapshot.flight.lock;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.common.JournalRecordUpdateEntryStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.journal.JournalService;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.LockSnapshotStep;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class SnapshotLockFlight extends Flight {
  public SnapshotLockFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);
    ApplicationContext appContext = (ApplicationContext) applicationContext;
    SnapshotDao snapshotDao = appContext.getBean(SnapshotDao.class);
    SnapshotService snapshotService = appContext.getBean(SnapshotService.class);
    JournalService journalService = appContext.getBean(JournalService.class);

    // Input parameters
    UUID snapshotId =
        UUID.fromString(inputParameters.get(JobMapKeys.SNAPSHOT_ID.getKeyName(), String.class));
    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    // Steps
    addStep(new LockSnapshotStep(snapshotDao, snapshotId, false));
    addStep(
        new JournalRecordUpdateEntryStep(
            journalService,
            userReq,
            snapshotId,
            IamResourceType.DATASNAPSHOT,
            "Snapshot locked",
            true));
    addStep(new SnapshotLockSetResponseStep(snapshotService, snapshotId));
  }
}
