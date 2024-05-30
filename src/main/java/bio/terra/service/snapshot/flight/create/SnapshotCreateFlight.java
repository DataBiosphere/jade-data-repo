package bio.terra.service.snapshot.flight.create;

import static bio.terra.common.FlightUtils.getDefaultExponentialBackoffRetryRule;
import static bio.terra.common.FlightUtils.getDefaultRandomBackoffRetryRule;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.logging.PerformanceLogger;
import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.GetResourceBufferProjectStep;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.common.JournalRecordUpdateEntryStep;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.duos.DuosDao;
import bio.terra.service.duos.DuosService;
import bio.terra.service.filedata.DrsIdService;
import bio.terra.service.filedata.DrsService;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.azure.tables.TableDao;
import bio.terra.service.filedata.azure.tables.TableDependencyDao;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.journal.JournalService;
import bio.terra.service.policy.PolicyService;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.flight.AuthorizeBillingProfileUseStep;
import bio.terra.service.profile.flight.VerifyBillingAccountAccessStep;
import bio.terra.service.profile.google.GoogleBillingService;
import bio.terra.service.resourcemanagement.BufferService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureContainerPdao;
import bio.terra.service.resourcemanagement.azure.AzureMonitoringService;
import bio.terra.service.resourcemanagement.flight.AzureStorageMonitoringStepProvider;
import bio.terra.service.resourcemanagement.google.GoogleResourceManagerService;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.UnlockSnapshotStep;
import bio.terra.service.snapshot.flight.authDomain.AddSnapshotAuthDomainStep;
import bio.terra.service.snapshot.flight.authDomain.CreateSnapshotGroupConstraintPolicyStep;
import bio.terra.service.snapshot.flight.duos.CreateDuosFirecloudGroupStep;
import bio.terra.service.snapshot.flight.duos.IfNoGroupRetrievedStep;
import bio.terra.service.snapshot.flight.duos.RecordDuosFirecloudGroupStep;
import bio.terra.service.snapshot.flight.duos.RetrieveDuosFirecloudGroupStep;
import bio.terra.service.snapshot.flight.duos.SyncDuosFirecloudGroupStep;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import bio.terra.service.snapshotbuilder.SnapshotRequestDao;
import bio.terra.service.tabulardata.google.bigquery.BigQuerySnapshotPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRule;
import bio.terra.stairway.RetryRuleExponentialBackoff;
import java.util.List;
import java.util.Objects;
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
    GoogleResourceManagerService googleResourceManagerService =
        appContext.getBean(GoogleResourceManagerService.class);
    JournalService journalService = appContext.getBean(JournalService.class);
    String tdrServiceAccountEmail = appContext.getBean("tdrServiceAccountEmail", String.class);
    DrsIdService drsIdService = appContext.getBean(DrsIdService.class);
    DrsService drsService = appContext.getBean(DrsService.class);
    DuosDao duosDao = appContext.getBean(DuosDao.class);
    DuosService duosService = appContext.getBean(DuosService.class);
    PolicyService policyService = appContext.getBean(PolicyService.class);
    ApplicationConfiguration appConfig = appContext.getBean(ApplicationConfiguration.class);
    AzureContainerPdao azureContainerPdao = appContext.getBean(AzureContainerPdao.class);
    AzureMonitoringService monitoringService = appContext.getBean(AzureMonitoringService.class);
    AzureStorageMonitoringStepProvider azureStorageMonitoringStepProvider =
        new AzureStorageMonitoringStepProvider(monitoringService);
    SnapshotRequestDao snapshotRequestDao = appContext.getBean(SnapshotRequestDao.class);
    SnapshotBuilderService snapshotBuilderService =
        appContext.getBean(SnapshotBuilderService.class);
    IamService iamService = appContext.getBean(IamService.class);

    SnapshotRequestModel snapshotReq =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), SnapshotRequestModel.class);
    String snapshotName = snapshotReq.getName();

    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    UUID snapshotId =
        Objects.requireNonNull(
            inputParameters.get(JobMapKeys.SNAPSHOT_ID.getKeyName(), UUID.class));

    RetryRule randomBackoffRetry =
        getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads());

    // TODO note that with multi-dataset snapshots this will need to change
    List<Dataset> sourceDatasets =
        snapshotService.getSourceDatasetsFromSnapshotRequest(snapshotReq);
    Dataset sourceDataset = sourceDatasets.get(0);
    UUID datasetId = sourceDataset.getId();
    String datasetName = sourceDataset.getName();

    var platform =
        CloudPlatformWrapper.of(sourceDataset.getDatasetSummary().getStorageCloudPlatform());

    // Take out a shared lock on the source dataset, to guard against it being deleted out from
    // under us (for example)
    addStep(new LockDatasetStep(datasetService, datasetId, true), randomBackoffRetry);

    // Make sure this user is authorized to use the billing profile in SAM
    addStep(
        new AuthorizeBillingProfileUseStep(profileService, snapshotReq.getProfileId(), userReq));

    if (platform.isGcp()) {
      addStep(new VerifyBillingAccountAccessStep(googleBillingService));

      // Get a new google project from RBS and store it in the working map
      addStep(
          new GetResourceBufferProjectStep(
              bufferService,
              googleResourceManagerService,
              sourceDataset.isSecureMonitoringEnabled()),
          getDefaultExponentialBackoffRetryRule());

      // Get or initialize the project where the snapshot resources will be created
      addStep(
          new CreateSnapshotInitializeProjectStep(
              resourceService, sourceDatasets, snapshotName, snapshotId),
          getDefaultExponentialBackoffRetryRule());
    }

    // if DUOS id in request, get/create firecloud group (needed for CreateSnapshotMetadataStep)
    String duosId = snapshotReq.getDuosId();
    if (duosId != null) {
      addStep(new RetrieveDuosFirecloudGroupStep(duosDao, duosId));
      addStep(
          new IfNoGroupRetrievedStep(
              new CreateDuosFirecloudGroupStep(duosService, iamClient, duosId)));
      addStep(new IfNoGroupRetrievedStep(new RecordDuosFirecloudGroupStep(duosDao)));
      addStep(new IfNoGroupRetrievedStep(new SyncDuosFirecloudGroupStep(duosService, duosId)));
      // the DUOS Firecloud group is added as a reader in SnapshotAuthzIamStep
    }

    // create the snapshot metadata object in postgres and lock it
    addStep(
        new CreateSnapshotMetadataStep(snapshotDao, snapshotService, snapshotReq, snapshotId),
        getDefaultExponentialBackoffRetryRule());

    if (platform.isAzure()) {
      addStep(
          new CreateSnapshotCreateAzureStorageAccountStep(
              resourceService, sourceDataset, snapshotId));
      addStep(new CreateSnapshotCreateAzureContainerStep(resourceService, azureContainerPdao));

      // Turn on logging and monitoring for the storage account associated with the snapshot
      azureStorageMonitoringStepProvider
          .configureSteps(
              sourceDataset.isSecureMonitoringEnabled(), sourceDataset.getStorageAccountRegion())
          .forEach(s -> this.addStep(s.step(), s.retryRule()));

      addStep(
          new CreateSnapshotSourceDatasetDataSourceAzureStep(
              azureSynapsePdao, azureBlobStorePdao, userReq));
      addStep(
          new CreateSnapshotTargetDataSourceAzureStep(
              azureSynapsePdao, azureBlobStorePdao, userReq));
    }

    // Make the big query dataset with views and populate row id filtering tables.
    // Depending on the type of snapshot, the primary data step will differ:
    // TODO: this assumes single-dataset snapshots, will need to add a loop for multiple
    switch (snapshotReq.getContents().get(0).getMode()) {
      case BYASSET -> {
        addStep(new CreateSnapshotValidateAssetStep(datasetService, snapshotService, snapshotReq));
        addStep(
            platform.choose(
                () ->
                    new CreateSnapshotPrimaryDataAssetGcpStep(
                        bigQuerySnapshotPdao, snapshotDao, snapshotService, snapshotReq),
                () ->
                    new CreateSnapshotByAssetParquetFilesAzureStep(
                        azureSynapsePdao, snapshotService, snapshotReq, snapshotId)));
      }
      case BYFULLVIEW -> addStep(
          platform.choose(
              () ->
                  new CreateSnapshotPrimaryDataFullViewGcpStep(
                      bigQuerySnapshotPdao,
                      datasetService,
                      snapshotDao,
                      snapshotService,
                      snapshotReq),
              () ->
                  new CreateSnapshotByFullViewParquetFilesAzureStep(
                      azureSynapsePdao, snapshotService, snapshotReq, snapshotId)));
      case BYQUERY -> {
        addStep(new CreateSnapshotValidateQueryStep(datasetService, snapshotReq));
        addStep(
            platform.choose(
                () ->
                    new CreateSnapshotPrimaryDataQueryGcpStep(
                        bigQuerySnapshotPdao,
                        snapshotService,
                        datasetService,
                        snapshotDao,
                        snapshotReq,
                        userReq),
                () ->
                    new CreateSnapshotByQueryParquetFilesAzureStep(
                        azureSynapsePdao,
                        snapshotDao,
                        snapshotService,
                        snapshotReq,
                        datasetService,
                        userReq,
                        snapshotId)));
      }
      case BYROWID -> addStep(
          platform.choose(
              () ->
                  new CreateSnapshotPrimaryDataRowIdsStep(
                      bigQuerySnapshotPdao, snapshotDao, snapshotService, snapshotReq),
              () ->
                  new CreateSnapshotByRowIdParquetFilesAzureStep(
                      azureSynapsePdao, snapshotService, snapshotReq, snapshotId)));
      case BYREQUESTID -> addStep(
          platform.choose(
              () ->
                  new CreateSnapshotByRequestIdGcpStep(
                      snapshotReq,
                      snapshotService,
                      snapshotBuilderService,
                      snapshotRequestDao,
                      snapshotDao,
                      userReq,
                      bigQuerySnapshotPdao),
              () ->
                  new CreateSnapshotByRequestIdAzureStep(
                      snapshotReq,
                      snapshotService,
                      snapshotBuilderService,
                      snapshotRequestDao,
                      snapshotDao,
                      userReq,
                      azureSynapsePdao,
                      snapshotId)));
    }
    if (platform.isAzure()) {
      addStep(
          new CreateSnapshotCreateRowIdParquetFileStep(
              azureSynapsePdao, snapshotService, snapshotId));
      addStep(
          new CreateSnapshotCountTableRowsAzureStep(snapshotDao, snapshotReq), randomBackoffRetry);
    }

    if (platform.isGcp()) {
      // compute the row counts for each of the snapshot tables and store in metadata
      addStep(
          new CountSnapshotTableRowsStep(bigQuerySnapshotPdao, snapshotDao, snapshotReq),
          randomBackoffRetry);
    }

    // Create the IAM resource and readers for the snapshot
    // The IAM code contains retries, so we don't make a retry rule here.
    addStep(new SnapshotAuthzIamStep(iamClient, snapshotService, snapshotReq, userReq, snapshotId));

    // add data access control groups to the snapshot
    List<String> dataAccessControlGroups = snapshotReq.getDataAccessControlGroups();
    if (Objects.nonNull(dataAccessControlGroups) && !dataAccessControlGroups.isEmpty()) {
      addStep(
          new CreateSnapshotGroupConstraintPolicyStep(
              policyService, snapshotId, dataAccessControlGroups));
      addStep(
          new AddSnapshotAuthDomainStep(iamService, userReq, snapshotId, dataAccessControlGroups));
    }

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
          new SnapshotAuthzTabularAclStep(
              bigQuerySnapshotPdao, snapshotService, configService, snapshotId),
          pdaoAclRetryRule);

      // Apply the IAM readers to the GCS files
      if (!sourceDataset.isSelfHosted()) {
        addStep(
            new SnapshotAuthzFileAclStep(
                dependencyDao, snapshotService, gcsPdao, datasetService, configService, snapshotId),
            pdaoAclRetryRule);
      }

      addStep(new SnapshotAuthzBqJobUserStep(snapshotService, resourceService, snapshotName));
      addStep(
          new SnapshotAuthzServiceAccountConsumerStep(
              snapshotService, resourceService, snapshotName, tdrServiceAccountEmail));
      // Record the Drs IDs if this is a global file id snapshot
      if (snapshotReq.isGlobalFileIds()) {
        addStep(
            new SnapshotRecordFileIdsGcpStep(
                snapshotService, datasetService, drsIdService, drsService, fileDao, snapshotId));
      }
    } else if (platform.isAzure()) {
      addStep(
          new CreateSnapshotStorageTableDataStep(
              tableDao,
              azureAuthService,
              azureSynapsePdao,
              snapshotService,
              datasetId,
              datasetName,
              snapshotId));

      addStep(
          new CreateSnapshotStorageTableDependenciesStep(
              tableDependencyDao,
              azureAuthService,
              azureSynapsePdao,
              snapshotService,
              datasetId,
              snapshotId));
      // Calculate checksums and sizes for all directories in the snapshot
      addStep(
          new CreateSnapshotStorageTableComputeStep(
              tableDao, snapshotReq, snapshotService, azureAuthService));

      // Record the Drs IDs if this is a global file id snapshot
      if (snapshotReq.isGlobalFileIds()) {
        addStep(
            new SnapshotRecordFileIdsAzureStep(
                snapshotService,
                datasetService,
                drsIdService,
                drsService,
                tableDao,
                azureAuthService,
                snapshotId));
      }
      // cannot clean up azure synapse tables until after gathered refIds in
      // CreateSnapshotStorageTableDataStep
      addStep(
          new CreateSnapshotCleanSynapseAzureStep(azureSynapsePdao, snapshotService, snapshotId));
    }

    addStep(
        new CreateSnapshotPolicyStep(
            policyService, sourceDataset.isSecureMonitoringEnabled(), snapshotId));

    // unlock the resource metadata rows
    addStep(new UnlockSnapshotStep(snapshotDao, snapshotId));
    addStep(new UnlockDatasetStep(datasetService, datasetId, true));
    // once unlocked, the snapshot summary can be written as the job response
    addStep(new CreateSnapshotSetResponseStep(snapshotService, snapshotId));

    addStep(new CreateSnapshotJournalEntryStep(journalService, userReq, snapshotId));
    addStep(
        new JournalRecordUpdateEntryStep(
            journalService,
            userReq,
            datasetId,
            IamResourceType.DATASET,
            "A snapshot was created from this dataset."));
  }
}
