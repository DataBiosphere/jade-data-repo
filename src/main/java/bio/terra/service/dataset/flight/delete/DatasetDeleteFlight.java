package bio.terra.service.dataset.flight.delete;

import static bio.terra.common.FlightUtils.getDefaultExponentialBackoffRetryRule;
import static bio.terra.common.FlightUtils.getDefaultRandomBackoffRetryRule;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.DatasetBucketDao;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.azure.tables.TableDao;
import bio.terra.service.filedata.azure.tables.TableDependencyDao;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.iam.IamService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRule;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class DatasetDeleteFlight extends Flight {

  public DatasetDeleteFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    // get the required daos to pass into the steps
    ApplicationContext appContext = (ApplicationContext) applicationContext;
    DatasetBucketDao datasetBucketDao = appContext.getBean(DatasetBucketDao.class);
    DatasetDao datasetDao = appContext.getBean(DatasetDao.class);
    SnapshotDao snapshotDao = appContext.getBean(SnapshotDao.class);
    BigQueryDatasetPdao bigQueryDatasetPdao = appContext.getBean(BigQueryDatasetPdao.class);
    GcsPdao gcsPdao = appContext.getBean(GcsPdao.class);
    AzureBlobStorePdao azureBlobStorePdao = appContext.getBean(AzureBlobStorePdao.class);
    ResourceService resourceService = appContext.getBean(ResourceService.class);
    FireStoreDependencyDao dependencyDao = appContext.getBean(FireStoreDependencyDao.class);
    FireStoreDao fileDao = appContext.getBean(FireStoreDao.class);
    IamService iamClient = appContext.getBean(IamService.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    ConfigurationService configService = appContext.getBean(ConfigurationService.class);
    ApplicationConfiguration appConfig = appContext.getBean(ApplicationConfiguration.class);
    TableDao tableDao = appContext.getBean(TableDao.class);
    ProfileDao profileDao = appContext.getBean(ProfileDao.class);
    TableDependencyDao tableDependencyDao = appContext.getBean(TableDependencyDao.class);
    AzureAuthService azureAuthService = appContext.getBean(AzureAuthService.class);

    // get data from inputs that steps need
    UUID datasetId =
        UUID.fromString(inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class));
    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);
    var platform =
        CloudPlatformWrapper.of(
            inputParameters.get(JobMapKeys.CLOUD_PLATFORM.getKeyName(), String.class));
    RetryRule lockDatasetRetry =
        getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads());
    RetryRule primaryDataDeleteRetry = getDefaultExponentialBackoffRetryRule();

    if (configService.testInsertFault(ConfigEnum.DATASET_DELETE_LOCK_CONFLICT_SKIP_RETRY_FAULT)) {
      addStep(new LockDatasetStep(datasetService, datasetId, false, true));
    } else {
      addStep(new LockDatasetStep(datasetService, datasetId, false, true), lockDatasetRetry);
    }
    if (platform.isGcp()) {
      addStep(new DeleteDatasetStoreProjectIdStep(datasetId, datasetService, datasetBucketDao));
      // TODO: Do this check for Azure datasets
      addStep(
          new DeleteDatasetGcpValidateStep(snapshotDao, dependencyDao, datasetService, datasetId));
      addStep(
          new DeleteDatasetPrimaryDataStep(
              bigQueryDatasetPdao, gcsPdao, fileDao, datasetService, datasetId, configService),
          primaryDataDeleteRetry);
      // Delete access control on objects that were explicitly added by data repo operations.
      // Do this before delete resource from SAM to ensure we can get the metadata needed to
      // perform the operation. Also need to run before metadata is deleted since it is
      // required by the step.
      addStep(
          new DeleteDatasetAuthzBqAclsStep(
              iamClient, datasetService, resourceService, datasetId, userReq));
    } else if (platform.isAzure()) {
      addStep(
          new DeleteDatasetAzureValidateStep(
              snapshotDao,
              dependencyDao,
              datasetService,
              datasetId,
              tableDependencyDao,
              azureAuthService,
              profileDao,
              resourceService));
      addStep(
          new DeleteDatasetAzurePrimaryDataStep(
              azureBlobStorePdao,
              tableDao,
              datasetService,
              datasetId,
              configService,
              resourceService,
              profileDao,
              userReq),
          primaryDataDeleteRetry);
      addStep(
          new DeleteDatasetDeleteStorageAccountsStep(resourceService, datasetService, datasetId));
    }
    addStep(new DeleteDatasetMetadataStep(datasetDao, datasetId));
    addStep(new DeleteDatasetAuthzResource(iamClient, datasetId, userReq));

    // delete dataset project
    if (platform.isGcp()) {
      addStep(new DeleteDatasetMarkProjectStep(resourceService));
      addStep(new DeleteDatasetDeleteProjectStep(resourceService));
      addStep(new DeleteDatasetProjectMetadataStep(resourceService));
    }

    addStep(new UnlockDatasetStep(datasetService, datasetId, false), lockDatasetRetry);
  }
}
