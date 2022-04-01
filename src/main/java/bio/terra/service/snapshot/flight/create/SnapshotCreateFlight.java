package bio.terra.service.snapshot.flight.create;

import static bio.terra.common.FlightUtils.getDefaultExponentialBackoffRetryRule;

import bio.terra.app.logging.PerformanceLogger;
import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.GetResourceBufferProjectStep;
import bio.terra.common.exception.FeatureNotImplementedException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.azure.tables.TableDao;
import bio.terra.service.filedata.azure.tables.TableDependencyDao;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.iam.IamService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.flight.AuthorizeBillingProfileUseStep;
import bio.terra.service.profile.flight.VerifyBillingAccountAccessStep;
import bio.terra.service.profile.google.GoogleBillingService;
import bio.terra.service.resourcemanagement.BufferService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.exception.InvalidSnapshotException;
import bio.terra.service.snapshot.flight.UnlockSnapshotStep;
import bio.terra.service.tabulardata.google.bigquery.BigQuerySnapshotPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRule;
import bio.terra.stairway.RetryRuleExponentialBackoff;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.springframework.context.ApplicationContext;

public class SnapshotCreateFlight extends Flight {

  public SnapshotCreateFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    // get the required objects to pass into the steps
    ApplicationContext appContext = (ApplicationContext) applicationContext;
    BufferService bufferService = appContext.getBean(BufferService.class);
    SnapshotDao snapshotDao = appContext.getBean(SnapshotDao.class);
    SnapshotService snapshotService = appContext.getBean(SnapshotService.class);
    BigQuerySnapshotPdao bigQuerySnapshotPdao = appContext.getBean(BigQuerySnapshotPdao.class);
    FireStoreDependencyDao dependencyDao = appContext.getBean(FireStoreDependencyDao.class);
    FireStoreDao fileDao = appContext.getBean(FireStoreDao.class);
    IamService iamClient = appContext.getBean(IamService.class);
    GcsPdao gcsPdao = appContext.getBean(GcsPdao.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    ConfigurationService configService = appContext.getBean(ConfigurationService.class);
    ResourceService resourceService = appContext.getBean(ResourceService.class);
    PerformanceLogger performanceLogger = appContext.getBean(PerformanceLogger.class);
    ProfileService profileService = appContext.getBean(ProfileService.class);
    AzureSynapsePdao azureSynapsePdao = appContext.getBean(AzureSynapsePdao.class);
    AzureBlobStorePdao azureBlobStorePdao = appContext.getBean(AzureBlobStorePdao.class);
    TableDao tableDao = appContext.getBean(TableDao.class);
    AzureAuthService azureAuthService = appContext.getBean(AzureAuthService.class);
    TableDependencyDao tableDependencyDao = appContext.getBean(TableDependencyDao.class);
    GoogleBillingService googleBillingService = appContext.getBean(GoogleBillingService.class);

    SnapshotRequestModel snapshotReq =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), SnapshotRequestModel.class);
    String snapshotName = snapshotReq.getName();

    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    // TODO note that with multi-dataset snapshots this will need to change
    List<Dataset> sourceDatasets =
        snapshotService.getSourceDatasetsFromSnapshotRequest(snapshotReq);
    Dataset sourceDataset = sourceDatasets.get(0);
    UUID datasetId = sourceDataset.getId();
    String datasetName = sourceDataset.getName();

    var platform =
        CloudPlatformWrapper.of(sourceDataset.getDatasetSummary().getStorageCloudPlatform());

    // Make sure this user is authorized to use the billing profile in SAM
    addStep(
        new AuthorizeBillingProfileUseStep(profileService, snapshotReq.getProfileId(), userReq));

    // mint a snapshot id and put it in the working map
    addStep(new CreateSnapshotIdStep(snapshotReq));

    if (platform.isGcp()) {
      addStep(new VerifyBillingAccountAccessStep(googleBillingService));

      // Get a new google project from RBS and store it in the working map
      addStep(
          new GetResourceBufferProjectStep(
              bufferService, sourceDataset.isSecureMonitoringEnabled()));

      // Get or initialize the project where the snapshot resources will be created
      addStep(
          new CreateSnapshotInitializeProjectStep(resourceService, sourceDatasets, snapshotName),
          getDefaultExponentialBackoffRetryRule());
    }

    // create the snapshot metadata object in postgres and lock it
    addStep(
        new CreateSnapshotMetadataStep(snapshotDao, snapshotService, snapshotReq),
        getDefaultExponentialBackoffRetryRule());

    if (platform.isAzure()) {
      addStep(
          new CreateSnapshotCreateAzureStorageAccountStep(
              resourceService, sourceDataset, snapshotReq));
    }

    // Make the big query dataset with views and populate row id filtering tables.
    // Depending on the type of snapshot, the primary data step will differ:
    // TODO: this assumes single-dataset snapshots, will need to add a loop for multiple
    switch (snapshotReq.getContents().get(0).getMode()) {
      case BYASSET:
        if (platform.isGcp()) {
          addStep(
              new CreateSnapshotValidateAssetStep(datasetService, snapshotService, snapshotReq));
          addStep(
              new CreateSnapshotPrimaryDataAssetStep(
                  bigQuerySnapshotPdao, snapshotDao, snapshotService, snapshotReq));
          break;
        } else {
          throw new FeatureNotImplementedException(
              "By Asset Snapshots are not yet supported in Azure datasets.");
        }
      case BYFULLVIEW:
        if (platform.isGcp()) {
          addStep(
              new CreateSnapshotPrimaryDataFullViewGcpStep(
                  bigQuerySnapshotPdao, datasetService, snapshotDao, snapshotService, snapshotReq));
        } else if (platform.isAzure()) {
          addStep(
              new CreateSnapshotSourceDatasetDataSourceAzureStep(
                  azureSynapsePdao, azureBlobStorePdao, userReq));
          addStep(
              new CreateSnapshotTargetDataSourceAzureStep(
                  azureSynapsePdao, azureBlobStorePdao, userReq));
          addStep(new CreateSnapshotParquetFilesAzureStep(azureSynapsePdao, snapshotService));
          addStep(
              new CreateSnapshotCountTableRowsAzureStep(
                  azureSynapsePdao, snapshotDao, snapshotReq));
        }
        break;
      case BYQUERY:
        if (platform.isGcp()) {
          addStep(new CreateSnapshotValidateQueryStep(datasetService, snapshotReq));
          addStep(
              new CreateSnapshotPrimaryDataQueryStep(
                  bigQuerySnapshotPdao,
                  datasetService,
                  snapshotService,
                  snapshotDao,
                  snapshotReq,
                  userReq));
          break;
        } else {
          throw new FeatureNotImplementedException(
              "By Query Snapshots are not yet supported in Azure datasets.");
        }

      case BYROWID:
        if (platform.isGcp()) {
          addStep(
              new CreateSnapshotPrimaryDataRowIdsStep(
                  bigQuerySnapshotPdao, snapshotDao, snapshotService, snapshotReq));
          break;
        } else if (platform.isAzure()) {
          addStep(
              new CreateSnapshotSourceDatasetDataSourceAzureStep(
                  azureSynapsePdao, azureBlobStorePdao, userReq));
          addStep(
              new CreateSnapshotTargetDataSourceAzureStep(
                  azureSynapsePdao, azureBlobStorePdao, userReq));
          addStep(
              new CreateSnapshotByRowIdParquetFilesAzureStep(
                  azureSynapsePdao, snapshotService, snapshotReq));
          addStep(
              new CreateSnapshotCountTableRowsAzureStep(
                  azureSynapsePdao, snapshotDao, snapshotReq));
        }
        break;
      default:
        throw new InvalidSnapshotException("Snapshot does not have required mode information");
    }

    if (platform.isGcp()) {
      // compute the row counts for each of the snapshot tables and store in metadata
      addStep(new CountSnapshotTableRowsStep(bigQuerySnapshotPdao, snapshotDao, snapshotReq));
    }

    // Create the IAM resource and readers for the snapshot
    // The IAM code contains retries, so we don't make a retry rule here.
    addStep(new SnapshotAuthzIamStep(iamClient, snapshotService, snapshotReq, userReq));

    if (platform.isGcp()) {
      // Make the firestore file system for the snapshot
      addStep(
          new CreateSnapshotFireStoreDataStep(
              bigQuerySnapshotPdao,
              snapshotService,
              dependencyDao,
              datasetService,
              snapshotReq,
              fileDao,
              performanceLogger));

      // Calculate checksums and sizes for all directories in the snapshot
      RetryRule pdaoFirestoreDirCalcRetryRule =
          new RetryRuleExponentialBackoff(2, 30, TimeUnit.MINUTES.toSeconds(30));
      addStep(
          new CreateSnapshotFireStoreComputeStep(snapshotService, snapshotReq, fileDao),
          pdaoFirestoreDirCalcRetryRule);

      // Google says that ACL change propagation happens in a few seconds, but can take 5-7 minutes.
      // The max
      // operation timeout is generous.
      RetryRule pdaoAclRetryRule = getDefaultExponentialBackoffRetryRule();

      // Apply the IAM readers to the BQ dataset
      addStep(
          new SnapshotAuthzTabularAclStep(bigQuerySnapshotPdao, snapshotService, configService),
          pdaoAclRetryRule);

      // Apply the IAM readers to the GCS files
      addStep(
          new SnapshotAuthzFileAclStep(
              dependencyDao, snapshotService, gcsPdao, datasetService, configService),
          pdaoAclRetryRule);

      addStep(new SnapshotAuthzBqJobUserStep(snapshotService, resourceService, snapshotName));
    } else if (platform.isAzure()) {
      addStep(
          new CreateSnapshotStorageTableDataStep(
              tableDao,
              azureAuthService,
              azureSynapsePdao,
              snapshotService,
              datasetId,
              datasetName));

      addStep(
          new CreateSnapshotStorageTableDependenciesStep(
              tableDependencyDao, azureAuthService, azureSynapsePdao, snapshotService, datasetId));
      // Calculate checksums and sizes for all directories in the snapshot
      addStep(
          new CreateSnapshotStorageTableComputeStep(
              tableDao, snapshotReq, snapshotService, azureAuthService));
      // cannot clean up azure synapse tables until after gathered refIds in
      // CreateSnapshotStorageTableDataStep
      addStep(new CreateSnapshotCleanSynapseAzureStep(azureSynapsePdao, snapshotService));
    }

    // unlock the snapshot metadata row
    addStep(new UnlockSnapshotStep(snapshotDao, null));
  }
}
