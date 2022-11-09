package bio.terra.service.snapshot.flight.delete;

import static bio.terra.common.FlightUtils.getDefaultRandomBackoffRetryRule;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.common.JournalRecordDeleteEntryStep;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.filedata.azure.tables.TableDependencyDao;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.journal.JournalService;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountService;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.LockSnapshotStep;
import bio.terra.service.snapshot.flight.UnlockSnapshotStep;
import bio.terra.service.tabulardata.google.bigquery.BigQuerySnapshotPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRule;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class SnapshotDeleteFlight extends Flight {

  public SnapshotDeleteFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    // get the required daos to pass into the steps
    ApplicationContext appContext = (ApplicationContext) applicationContext;
    SnapshotDao snapshotDao = appContext.getBean(SnapshotDao.class);
    SnapshotService snapshotService = appContext.getBean(SnapshotService.class);
    FireStoreDependencyDao dependencyDao = appContext.getBean(FireStoreDependencyDao.class);
    FireStoreDao fileDao = appContext.getBean(FireStoreDao.class);
    BigQuerySnapshotPdao bigQuerySnapshotPdao = appContext.getBean(BigQuerySnapshotPdao.class);
    ResourceService resourceService = appContext.getBean(ResourceService.class);
    IamService iamClient = appContext.getBean(IamService.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    ConfigurationService configService = appContext.getBean(ConfigurationService.class);
    ApplicationConfiguration appConfig = appContext.getBean(ApplicationConfiguration.class);
    TableDependencyDao tableDependencyDao = appContext.getBean(TableDependencyDao.class);
    ProfileService profileService = appContext.getBean(ProfileService.class);
    AzureAuthService azureAuthService = appContext.getBean(AzureAuthService.class);
    AzureStorageAccountService azureStorageAccountService =
        appContext.getBean(AzureStorageAccountService.class);
    JournalService journalService = appContext.getBean(JournalService.class);
    String tdrServiceAccountEmail = appContext.getBean("tdrServiceAccountEmail", String.class);

    RetryRule randomBackoffRetry =
        getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads());

    UUID snapshotId =
        UUID.fromString(inputParameters.get(JobMapKeys.SNAPSHOT_ID.getKeyName(), String.class));
    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    // Lock the source dataset while deleting ACLs to avoid a race condition
    // Skip this step if the snapshot was already deleted
    // TODO note that with multi-dataset snapshots this will need to change
    addStep(new LockSnapshotStep(snapshotDao, snapshotId, true));
    addStep(
        new DeleteSnapshotPopAndLockDatasetStep(
            snapshotId, snapshotService, datasetService, userReq, false));

    // store project id
    addStep(new PerformGcpStep(new DeleteSnapshotStoreProjectIdStep(snapshotId, snapshotService)));

    // Delete access control on objects that were explicitly added by data repo operations.  Do
    // this before delete
    // resource from SAM to ensure we can get the metadata needed to perform the operation
    addStep(
        new PerformGcpStep(
            new DeleteSnapshotAuthzBqAclsStep(
                iamClient, resourceService, snapshotService, snapshotId, userReq)));
    addStep(
        new PerformGcpStep(
            new DeleteSnapshotAuthzServiceUsageAclsStep(
                iamClient,
                resourceService,
                snapshotService,
                snapshotId,
                userReq,
                tdrServiceAccountEmail)));

    // Delete access control first so Readers and Discoverers can no longer see snapshot
    // Google auto-magically removes the ACLs from BQ objects when SAM
    // deletes the snapshot group, so no ACL cleanup is needed beyond that.
    addStep(new DeleteSnapshotAuthzResource(iamClient, snapshotId, userReq));

    // Primary Data Deletion
    // Note: Must delete primary data before metadata; it relies on being able to retrieve the
    // snapshot object from the metadata to know what to delete.
    // --- GCP ---
    addStep(
        new PerformGCPDatasetDependencyStep(
            new DeleteSnapshotSourceDatasetDataGcpStep(
                dependencyDao, bigQuerySnapshotPdao, snapshotId, datasetService, snapshotService)),
        randomBackoffRetry);
    addStep(
        new PerformGcpStep(
            new DeleteSnapshotPrimaryDataGcpStep(
                bigQuerySnapshotPdao, snapshotService, fileDao, snapshotId, configService)),
        randomBackoffRetry);
    // --- Azure --
    addStep(
        new PerformAzureDatasetDependencyStep(
            new DeleteSnapshotDependencyDataAzureStep(
                tableDependencyDao,
                snapshotId,
                datasetService,
                profileService,
                resourceService,
                azureAuthService)),
        randomBackoffRetry);
    addStep(
        new PerformAzureStep(
            new DeleteSnapshotDeleteStorageAccountStep(
                snapshotId, resourceService, azureStorageAccountService)));

    // Delete Metadata
    addStep(new DeleteSnapshotMetadataStep(snapshotDao, snapshotId, userReq));
    addStep(new PerformAzureStep(new DeleteSnapshotMetadataAzureStep(azureStorageAccountService)));
    addStep(new PerformSnapshotStep(new UnlockSnapshotStep(snapshotDao, snapshotId)));

    // delete snapshot project
    addStep(
        new PerformGcpStep(
            new DeleteSnapshotMarkProjectStep(resourceService, snapshotId, snapshotService)));
    addStep(new PerformGcpStep(new DeleteSnapshotDeleteProjectStep(resourceService)));
    addStep(new PerformGcpStep(new DeleteSnapshotProjectMetadataStep(resourceService)));

    addStep(new PerformDatasetStep(new UnlockDatasetStep(datasetService, false)));
    addStep(
        new JournalRecordDeleteEntryStep(
            journalService,
            userReq,
            snapshotId,
            IamResourceType.DATASNAPSHOT,
            "Deleted snapshot."));
  }
}
