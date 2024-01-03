package bio.terra.service.snapshot.flight.authDomain;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.common.JournalRecordUpdateEntryStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.journal.JournalService;
import bio.terra.service.policy.PolicyService;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.flight.LockSnapshotStep;
import bio.terra.service.snapshot.flight.UnlockSnapshotStep;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

/**
 * This flight adds data access control groups to a snapshot. The provided user groups will be
 * registered as a group constraint policy in Terra Policy Service, and as an auth domain in Sam. If
 * a snapshot has data access controls, a user must have access to the resource directly (via a
 * policy) and belong to all of its data access control groups in order to view and access the data.
 */
public class SnapshotAddDataAccessControlsFlight extends Flight {

  public SnapshotAddDataAccessControlsFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    ApplicationContext appContext = (ApplicationContext) applicationContext;
    IamService iamService = appContext.getBean(IamService.class);
    JournalService journalService = appContext.getBean(JournalService.class);
    PolicyService policyService = appContext.getBean(PolicyService.class);
    SnapshotDao snapshotDao = appContext.getBean(SnapshotDao.class);

    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);
    UUID snapshotId =
        UUID.fromString(inputParameters.get(JobMapKeys.SNAPSHOT_ID.getKeyName(), String.class));
    List<String> userGroups = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), List.class);
    List<String> uniqueUserGroups = new HashSet<>(userGroups).stream().toList();

    addStep(new LockSnapshotStep(snapshotDao, snapshotId, true));

    addStep(
        new CreateSnapshotGroupConstraintPolicyStep(policyService, snapshotId, uniqueUserGroups));

    addStep(new AddSnapshotAuthDomainStep(iamService, userReq, snapshotId, uniqueUserGroups));

    addStep(new UnlockSnapshotStep(snapshotDao, snapshotId));

    addStep(
        new JournalRecordUpdateEntryStep(
            journalService,
            userReq,
            snapshotId,
            IamResourceType.DATASNAPSHOT,
            "Data access control groups were added to this snapshot."));

    addStep(new AddSnapshotAuthDomainSetResponseStep(iamService, userReq, snapshotId));
  }
}
