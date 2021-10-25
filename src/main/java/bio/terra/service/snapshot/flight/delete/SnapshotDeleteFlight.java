package bio.terra.service.snapshot.flight.delete;

import static bio.terra.common.FlightUtils.getDefaultRandomBackoffRetryRule;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.CloudPlatformWrapper;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.filedata.azure.tables.TableDependencyDao;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountService;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.LockSnapshotStep;
import bio.terra.service.snapshot.flight.UnlockSnapshotStep;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRule;
import java.util.List;
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
    BigQueryPdao bigQueryPdao = appContext.getBean(BigQueryPdao.class);
    ResourceService resourceService = appContext.getBean(ResourceService.class);
    IamService iamClient = appContext.getBean(IamService.class);
    DatasetDao datasetDao = appContext.getBean(DatasetDao.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    ConfigurationService configService = appContext.getBean(ConfigurationService.class);
    ApplicationConfiguration appConfig = appContext.getBean(ApplicationConfiguration.class);
    TableDependencyDao tableDependencyDao = appContext.getBean(TableDependencyDao.class);
    ProfileService profileService = appContext.getBean(ProfileService.class);
    AzureAuthService azureAuthService = appContext.getBean(AzureAuthService.class);
    AzureStorageAccountService azureStorageAccountService =
        appContext.getBean(AzureStorageAccountService.class);

    RetryRule randomBackoffRetry =
        getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads());

    UUID snapshotId =
        UUID.fromString(inputParameters.get(JobMapKeys.SNAPSHOT_ID.getKeyName(), String.class));
    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    // Lock the source dataset while deleting ACLs to avoid a race condition
    // Skip this step if the snapshot was already deleted
    // TODO note that with multi-dataset snapshots this will need to change
    List<Dataset> sourceDatasets = snapshotService.getSourceDatasetsFromSnapshotId(snapshotId);
    Dataset sourceDataset = sourceDatasets.get(0);
    UUID datasetId = sourceDataset.getId();
    CloudPlatformWrapper platform =
        CloudPlatformWrapper.of(sourceDataset.getDatasetSummary().getStorageCloudPlatform());

    addStep(new LockDatasetStep(datasetDao, datasetId, false));

    addStep(new LockSnapshotStep(snapshotDao, snapshotId, true));

    if (platform.isGcp()) {
      // Delete access control on objects that were explicitly added by data repo operations.  Do
      // this
      // before delete
      // resource from SAM to ensure we can get the metadata needed to perform the operation
      addStep(
          new DeleteSnapshotAuthzBqAclsStep(
              iamClient, resourceService, snapshotService, snapshotId, userReq));
    }
    // Delete access control first so Readers and Discoverers can no longer see snapshot
    // Google auto-magically removes the ACLs from BQ objects when SAM
    // deletes the snapshot group, so no ACL cleanup is needed beyond that.
    addStep(new DeleteSnapshotAuthzResource(iamClient, snapshotId, userReq));

    // Must delete primary data before metadata; it relies on being able to retrieve the
    // snapshot object from the metadata to know what to delete.
    if (platform.isGcp()) {
      addStep(
          new DeleteSnapshotPrimaryDataStep(
              bigQueryPdao,
              snapshotService,
              dependencyDao,
              fileDao,
              snapshotId,
              datasetService,
              configService),
          randomBackoffRetry);
    } else if (platform.isAzure()) {
      addStep(
          new DeleteSnapshotDependencyDataAzureStep(
              snapshotService,
              tableDependencyDao,
              snapshotId,
              datasetService,
              profileService,
              resourceService,
              azureAuthService));
      // TODO with DR-2127 - Add check to see if anything else uses storage account before deleting
      // and add step that removes (1) Storage Tables for the snapshot & (2) The metadata container
      // blob for the snapshot
      addStep(
          new DeleteSnapshotDeleteStorageAccountStep(
              snapshotId, resourceService, azureStorageAccountService));
    }

    addStep(new DeleteSnapshotMetadataStep(snapshotDao, snapshotId));
    addStep(new UnlockSnapshotStep(snapshotDao, snapshotId));

    addStep(new UnlockDatasetStep(datasetDao, datasetId, false));
  }
}
