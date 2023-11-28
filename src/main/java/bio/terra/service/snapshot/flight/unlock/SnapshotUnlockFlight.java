package bio.terra.service.snapshot.flight.unlock;

import static bio.terra.common.FlightUtils.getDefaultRandomBackoffRetryRule;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.common.JournalRecordUpdateEntryStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.journal.JournalService;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.UnlockSnapshotStep;
import bio.terra.service.snapshot.flight.lock.SnapshotLockSetResponseStep;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRule;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class SnapshotUnlockFlight extends Flight {
  public SnapshotUnlockFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);
    ApplicationContext appContext = (ApplicationContext) applicationContext;
    ApplicationConfiguration appConfig = appContext.getBean(ApplicationConfiguration.class);
    SnapshotService snapshotService = appContext.getBean(SnapshotService.class);
    SnapshotDao snapshotDao = appContext.getBean(SnapshotDao.class);
    JournalService journalService = appContext.getBean(JournalService.class);

    // Input parameters
    UUID snapshotId =
        UUID.fromString(inputParameters.get(JobMapKeys.SNAPSHOT_ID.getKeyName(), String.class));
    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);
    String lockName = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), String.class);

    // Configurations
    RetryRule unlockSnapshotRetry =
        getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads());

    // Steps
    addStep(new UnlockSnapshotStep(snapshotDao, snapshotId, lockName, true), unlockSnapshotRetry);
    addStep(
        new JournalRecordUpdateEntryStep(
            journalService, userReq, snapshotId, IamResourceType.DATASNAPSHOT, "Snapshot locked."));
    addStep(new SnapshotLockSetResponseStep(snapshotService, snapshotId));
  }
}
