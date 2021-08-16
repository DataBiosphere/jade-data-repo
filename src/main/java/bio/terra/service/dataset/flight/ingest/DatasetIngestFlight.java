package bio.terra.service.dataset.flight.ingest;

import static bio.terra.common.FlightUtils.getDefaultRandomBackoffRetryRule;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.CloudPlatformWrapper;
import bio.terra.model.CloudPlatform;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.flight.AuthorizeBillingProfileUseStep;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRule;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class DatasetIngestFlight extends Flight {

  public DatasetIngestFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    // get the required daos to pass into the steps
    ApplicationContext appContext = (ApplicationContext) applicationContext;
    DatasetDao datasetDao = appContext.getBean(DatasetDao.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    BigQueryPdao bigQueryPdao = appContext.getBean(BigQueryPdao.class);
    FireStoreDao fileDao = appContext.getBean(FireStoreDao.class);
    ConfigurationService configService = appContext.getBean(ConfigurationService.class);
    ApplicationConfiguration appConfig = appContext.getBean(ApplicationConfiguration.class);
    ProfileService profileService = appContext.getBean(ProfileService.class);
    AzureSynapsePdao azureSynapsePdao = appContext.getBean(AzureSynapsePdao.class);
    ResourceService resourceService = appContext.getBean(ResourceService.class);

    IngestRequestModel ingestRequestModel =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), IngestRequestModel.class);
    UUID datasetId =
        UUID.fromString(inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class));
    CloudPlatformWrapper cloudPlatform =
        CloudPlatformWrapper.of(
            datasetService.retrieve(datasetId).getDatasetSummary().getStorageCloudPlatform());
    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    RetryRule lockDatasetRetry =
        getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads());

    // TODO - we'll need to implement this check for Azure
    addStep(
        new AuthorizeBillingProfileUseStep(
            profileService, ingestRequestModel.getProfileId(), userReq));

    addStep(new LockDatasetStep(datasetDao, datasetId, true), lockDatasetRetry);
    addStep(new IngestSetupStep(datasetService, configService, cloudPlatform));

    if (cloudPlatform.is(CloudPlatform.GCP)) {
      addStep(new IngestLoadTableStep(datasetService, bigQueryPdao));
      addStep(new IngestRowIdsStep(datasetService, bigQueryPdao));
      // TODO - For Azure, we'll cover this with DR-2017
      addStep(new IngestValidateRefsStep(datasetService, bigQueryPdao, fileDao));
      addStep(new IngestInsertIntoDatasetTableStep(datasetService, bigQueryPdao));
      addStep(new IngestCleanupStep(datasetService, bigQueryPdao));
    } else if (cloudPlatform.is(CloudPlatform.AZURE)) {
      addStep(new IngestCreateIngestRequestDataSourceStep(azureSynapsePdao));
      addStep(
          new IngestCreateTargetDataSourceStep(azureSynapsePdao, datasetService, resourceService));
      addStep(new IngestCreateParquetFilesStep(azureSynapsePdao, datasetService));
      addStep(new IngestCleanSynapseStep(azureSynapsePdao));
    }
    addStep(new UnlockDatasetStep(datasetDao, datasetId, true), lockDatasetRetry);
  }
}
