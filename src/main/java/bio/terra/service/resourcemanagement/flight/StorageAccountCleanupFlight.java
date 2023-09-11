package bio.terra.service.resourcemanagement.flight;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.ErrorCollector;
import bio.terra.common.ValidateBucketAccessStep;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.common.JournalRecordUpdateEntryStep;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetBucketDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetStorageAccountDao;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.dataset.flight.ingest.*;
import bio.terra.service.dataset.flight.transactions.TransactionCommitStep;
import bio.terra.service.dataset.flight.transactions.TransactionLockStep;
import bio.terra.service.dataset.flight.transactions.TransactionOpenStep;
import bio.terra.service.dataset.flight.transactions.TransactionUnlockStep;
import bio.terra.service.filedata.CloudFileReader;
import bio.terra.service.filedata.FileService;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.azure.tables.TableDirectoryDao;
import bio.terra.service.filedata.flight.ingest.*;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.job.JobService;
import bio.terra.service.journal.JournalService;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.flight.LoadLockStep;
import bio.terra.service.load.flight.LoadUnlockStep;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.flight.AuthorizeBillingProfileUseStep;
import bio.terra.service.profile.flight.VerifyBillingAccountAccessStep;
import bio.terra.service.profile.google.GoogleBillingService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureContainerPdao;
import bio.terra.service.resourcemanagement.azure.AzureMonitoringService;
import bio.terra.service.resourcemanagement.google.GoogleProjectService;
import bio.terra.service.tabulardata.azure.StorageTableService;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.service.tabulardata.google.bigquery.BigQueryTransactionPdao;
import bio.terra.stairway.*;
import org.springframework.context.ApplicationContext;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import static bio.terra.common.FlightUtils.getDefaultExponentialBackoffRetryRule;
import static bio.terra.common.FlightUtils.getDefaultRandomBackoffRetryRule;

public class StorageAccountCleanupFlight extends Flight {

  public StorageAccountCleanupFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    ApplicationContext appContext = (ApplicationContext) applicationContext;
    AzureMonitoringService monitoringService = appContext.getBean(AzureMonitoringService.class);

    UUID subscriptionId =
        inputParameters.get(JobMapKeys.SUBSCRIPTION_ID.getKeyName(), UUID.class);
    String resourceGroupName =
        inputParameters.get(JobMapKeys.RESOURCE_GROUP_NAME.getKeyName(), String.class);
    String storageAccountName =
        inputParameters.get(JobMapKeys.STORAGE_ACCOUNT_NAME.getKeyName(), String.class);
    ErrorCollector errorCollector = new ErrorCollector(3, "StorageAccountCleanupFlight");


    AzureStorageMonitoringStepProvider azureStorageMonitoringStepProvider =
        new AzureStorageMonitoringStepProvider(monitoringService);



    azureStorageMonitoringStepProvider
        .configureUndoSteps(subscriptionId, resourceGroupName, storageAccountName, errorCollector)
        .forEach(s -> this.addStep(s.step()));
  }

}
