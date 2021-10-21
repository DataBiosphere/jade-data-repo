package bio.terra.service.snapshot.flight.create;

import static bio.terra.common.FlightUtils.getDefaultExponentialBackoffRetryRule;

import bio.terra.app.logging.PerformanceLogger;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.GetResourceBufferProjectStep;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.flight.AuthorizeBillingProfileUseStep;
import bio.terra.service.resourcemanagement.BufferService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.exception.InvalidSnapshotException;
import bio.terra.service.snapshot.flight.UnlockSnapshotStep;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRule;
import java.util.List;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class SnapshotCreateFlight extends Flight {

  public SnapshotCreateFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    // get the required objects to pass into the steps
    ApplicationContext appContext = (ApplicationContext) applicationContext;
    BufferService bufferService = appContext.getBean(BufferService.class);
    SnapshotDao snapshotDao = appContext.getBean(SnapshotDao.class);
    SnapshotService snapshotService = appContext.getBean(SnapshotService.class);
    BigQueryPdao bigQueryPdao = appContext.getBean(BigQueryPdao.class);
    FireStoreDependencyDao dependencyDao = appContext.getBean(FireStoreDependencyDao.class);
    FireStoreDao fileDao = appContext.getBean(FireStoreDao.class);
    IamService iamClient = appContext.getBean(IamService.class);
    GcsPdao gcsPdao = appContext.getBean(GcsPdao.class);
    DatasetDao datasetDao = appContext.getBean(DatasetDao.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    ConfigurationService configService = appContext.getBean(ConfigurationService.class);
    ResourceService resourceService = appContext.getBean(ResourceService.class);
    PerformanceLogger performanceLogger = appContext.getBean(PerformanceLogger.class);
    ProfileService profileService = appContext.getBean(ProfileService.class);

    SnapshotRequestModel snapshotReq =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), SnapshotRequestModel.class);
    String snapshotName = snapshotReq.getName();

    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    // TODO note that with multi-dataset snapshots this will need to change
    List<Dataset> sourceDatasets =
        snapshotService.getSourceDatasetsFromSnapshotRequest(snapshotReq);
    Dataset sourceDataset = sourceDatasets.get(0);
    var platform =
        CloudPlatformWrapper.of(sourceDataset.getDatasetSummary().getStorageCloudPlatform());
    GoogleRegion firestoreRegion =
        (GoogleRegion)
            sourceDataset
                .getDatasetSummary()
                .getStorageResourceRegion(GoogleCloudResource.FIRESTORE);

    // Make sure this user is allowed to use the billing profile and that the underlying
    // billing information remains valid.
    addStep(
        new AuthorizeBillingProfileUseStep(profileService, snapshotReq.getProfileId(), userReq));

    // Get a new google project from RBS and store it in the working map
    addStep(new GetResourceBufferProjectStep(bufferService));

    // create the snapshot metadata object in postgres and lock it
    // mint a snapshot id and put it in the working map
    addStep(new CreateSnapshotIdStep(snapshotReq));

    // Get or initialize the project where the snapshot resources will be created
    addStep(
        new CreateSnapshotInitializeProjectStep(
            resourceService, firestoreRegion, sourceDatasets, snapshotName));

    addStep(new CreateSnapshotMetadataStep(snapshotDao, snapshotService, snapshotReq));

    // Make the big query dataset with views and populate row id filtering tables.
    // Depending on the type of snapshot, the primary data step will differ:
    // TODO: this assumes single-dataset snapshots, will need to add a loop for multiple
    switch (snapshotReq.getContents().get(0).getMode()) {
      case BYASSET:
        addStep(new CreateSnapshotValidateAssetStep(datasetService, snapshotService, snapshotReq));
        addStep(
            new CreateSnapshotPrimaryDataAssetStep(
                bigQueryPdao, snapshotDao, snapshotService, snapshotReq));
        break;
      case BYFULLVIEW:
        addStep(
            new CreateSnapshotPrimaryDataFullViewStep(
                bigQueryPdao, datasetService, snapshotDao, snapshotService, snapshotReq));
        break;
      case BYQUERY:
        addStep(new CreateSnapshotValidateQueryStep(datasetService, snapshotReq));
        addStep(
            new CreateSnapshotPrimaryDataQueryStep(
                bigQueryPdao, datasetService, snapshotService, snapshotDao, snapshotReq));
        break;
      case BYROWID:
        addStep(
            new CreateSnapshotPrimaryDataRowIdsStep(
                bigQueryPdao, snapshotDao, snapshotService, snapshotReq));
        break;
      default:
        throw new InvalidSnapshotException("Snapshot does not have required mode information");
    }

    // compute the row counts for each of the snapshot tables and store in metadata
    addStep(new CountSnapshotTableRowsStep(bigQueryPdao, snapshotDao, snapshotReq));

    // Create the IAM resource and readers for the snapshot
    // The IAM code contains retries, so we don't make a retry rule here.
    addStep(new SnapshotAuthzIamStep(iamClient, snapshotService, snapshotReq, userReq));

    // Make the firestore file system for the snapshot
    addStep(
        new CreateSnapshotFireStoreDataStep(
            bigQueryPdao,
            snapshotService,
            dependencyDao,
            datasetService,
            snapshotReq,
            fileDao,
            performanceLogger));

    // Calculate checksums and sizes for all directories in the snapshot
    addStep(new CreateSnapshotFireStoreComputeStep(snapshotService, snapshotReq, fileDao));

    // Lock the source dataset while adding ACLs to avoid a race condition
    // Add a retry in case an ingest flight is currently in progress on the dataset
    RetryRule lockDatasetRetryRule = getDefaultExponentialBackoffRetryRule();
    UUID datasetId = sourceDataset.getId();

    addStep(new LockDatasetStep(datasetDao, datasetId, false), lockDatasetRetryRule);

    // Google says that ACL change propagation happens in a few seconds, but can take 5-7 minutes.
    // The max
    // operation timeout is generous.
    RetryRule pdaoAclRetryRule = getDefaultExponentialBackoffRetryRule();

    // Apply the IAM readers to the BQ dataset
    addStep(
        new SnapshotAuthzTabularAclStep(bigQueryPdao, snapshotService, configService),
        pdaoAclRetryRule);

    // Apply the IAM readers to the GCS files
    if (platform.isGcp()) {
      addStep(
          new SnapshotAuthzFileAclStep(
              dependencyDao, snapshotService, gcsPdao, datasetService, configService),
          pdaoAclRetryRule);
    }

    addStep(new SnapshotAuthzBqJobUserStep(snapshotService, resourceService, snapshotName));

    // Unlock dataset
    addStep(new UnlockDatasetStep(datasetDao, datasetId, false));

    // unlock the snapshot metadata row
    addStep(new UnlockSnapshotStep(snapshotDao, null));
  }
}
