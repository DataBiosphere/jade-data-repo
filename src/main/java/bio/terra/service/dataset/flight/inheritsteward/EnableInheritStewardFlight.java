package bio.terra.service.dataset.flight.inheritsteward;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.common.JournalRecordUpdateEntryStep;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
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
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class EnableInheritStewardFlight extends Flight {
  public EnableInheritStewardFlight(FlightMap inputParameters, Object applicationContext) {
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
    UUID datasetId = inputParameters.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
    String custodianEmail =
        inputParameters.get(JobMapKeys.CUSTODIAN_EMAIL.getKeyName(), String.class);
    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    boolean inheritSteward = true;
    addStep(new LockDatasetStep(datasetService, datasetId, false));
    addStep(new SetInheritStewardFlagStep(datasetDao, datasetId, inheritSteward));
    addStep(new GetSnapshotIdsStep(snapshotDao, datasetId));
    addStep(new SetParentOnSnapshotsStep(iamService, datasetId, userReq));
    addStep(new GetSnapshotGoogleProjectIdsStep(snapshotService, datasetId));
    addStep(new SetAuthGcpUserRolesStep(resourceService, custodianEmail, inheritSteward));
    addStep(
        new SetAuthTabularAclStep(
            bigQuerySnapshotPdao, snapshotService, custodianEmail, inheritSteward));
    addStep(new AdjustStewardMembersStep(userReq, iamService, inheritSteward));
    addStep(new UnlockDatasetStep(datasetService, false));
    addStep(
        new JournalRecordUpdateEntryStep(
            journalService, userReq, datasetId, IamResourceType.DATASET, "Enable inherit steward"));
  }
}
