package bio.terra.service.dataset.flight.inheritsteward;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.common.JournalRecordUpdateEntryStep;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.journal.JournalService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.tabulardata.google.bigquery.BigQuerySnapshotPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import java.util.List;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class SetInheritStewardFlight extends Flight {
  public SetInheritStewardFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    // Get the required DAOs and services to pass into the steps
    ApplicationContext appContext = (ApplicationContext) applicationContext;
    DatasetDao datasetDao = appContext.getBean(DatasetDao.class);
    SnapshotDao snapshotDao = appContext.getBean(SnapshotDao.class);
    ResourceService resourceService = appContext.getBean(ResourceService.class);
    SnapshotService snapshotService = appContext.getBean(SnapshotService.class);
    BigQuerySnapshotPdao bigQuerySnapshotPdao = appContext.getBean(BigQuerySnapshotPdao.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    IamService iamService = appContext.getBean(IamService.class);
    JournalService journalService = appContext.getBean(JournalService.class);

    // Get the input parameters
    UUID datasetId = inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), UUID.class);
    List<String> datasetPolicyEmails =
        inputParameters.get(JobMapKeys.DATASET_POLICY_EMAILS.getKeyName(), List.class);
    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    boolean inheritSteward =
        inputParameters.get(JobMapKeys.INHERIT_STEWARD.getKeyName(), Boolean.class);
    addStep(new LockDatasetStep(datasetService, datasetId, false));
    addStep(new GetSnapshotIdsStep(snapshotDao, iamService, userReq, datasetId, inheritSteward));
    if (inheritSteward) {
      // Make sure no child snapshot have auth domains
      addStep(new CheckChildSnapshotAuthDomainStep(snapshotService, userReq));
      // If we are setting inherit steward to true, we want to set the flag first
      addStep(new SetInheritStewardFlagStep(datasetDao, datasetId, inheritSteward));
    }

    addStep(new SetParentOnSnapshotsStep(iamService, datasetId, userReq, inheritSteward));
    addStep(
        new SetAuthGcpUserRolesStep(
            resourceService, snapshotService, datasetPolicyEmails, inheritSteward));
    addStep(
        new SetAuthTabularAclStep(
            bigQuerySnapshotPdao, snapshotService, datasetPolicyEmails, inheritSteward));
    addStep(new AdjustStewardMembersStep(userReq, iamService, inheritSteward));
    if (!inheritSteward) {
      // If we are setting inherit steward to false, we want to set the flag last
      addStep(new SetInheritStewardFlagStep(datasetDao, datasetId, inheritSteward));
    }
    addStep(new UnlockDatasetStep(datasetService, datasetId, false));
    addStep(
        new JournalRecordUpdateEntryStep(
            journalService,
            userReq,
            datasetId,
            IamResourceType.DATASET,
            "Set inherit steward flag to " + inheritSteward));
  }
}
