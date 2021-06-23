package bio.terra.service.dataset.flight.delete;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.service.configuration.ConfigurationService;
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
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRule;
import org.springframework.context.ApplicationContext;

import java.util.UUID;

import static bio.terra.common.FlightUtils.getDefaultExponentialBackoffRetryRule;
import static bio.terra.common.FlightUtils.getDefaultRandomBackoffRetryRule;

public class DatasetDeleteFlight extends Flight {



    public DatasetDeleteFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        // get the required daos to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        DatasetDao datasetDao = (DatasetDao)appContext.getBean("datasetDao");
        SnapshotDao snapshotDao = (SnapshotDao)appContext.getBean("snapshotDao");
        BigQueryPdao bigQueryPdao = (BigQueryPdao)appContext.getBean("bigQueryPdao");
        GcsPdao gcsPdao = (GcsPdao)appContext.getBean("gcsPdao");
        ResourceService resourceService = (ResourceService) appContext.getBean("resourceService");
        FireStoreDependencyDao dependencyDao = (FireStoreDependencyDao)appContext.getBean("fireStoreDependencyDao");
        FireStoreDao fileDao = (FireStoreDao)appContext.getBean("fireStoreDao");
        IamService iamClient = (IamService)appContext.getBean("iamService");
        DatasetService datasetService = (DatasetService) appContext.getBean("datasetService");
        ConfigurationService configService = (ConfigurationService)appContext.getBean("configurationService");
        ApplicationConfiguration appConfig =
            (ApplicationConfiguration)appContext.getBean("applicationConfiguration");

        // get data from inputs that steps need
        UUID datasetId = UUID.fromString(inputParameters.get(
            JobMapKeys.DATASET_ID.getKeyName(), String.class));
        AuthenticatedUserRequest userReq = inputParameters.get(
            JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);
        RetryRule lockDatasetRetry = getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads());
        RetryRule primaryDataDeleteRetry = getDefaultExponentialBackoffRetryRule();

        addStep(new LockDatasetStep(datasetDao, datasetId, false, true),
            lockDatasetRetry);
        addStep(new DeleteDatasetValidateStep(snapshotDao, dependencyDao, datasetService, datasetId));
        addStep(new DeleteDatasetPrimaryDataStep(
            bigQueryPdao,
            gcsPdao,
            fileDao,
            datasetService,
            datasetId,
            configService), primaryDataDeleteRetry);
        // Delete access control on objects that were explicitly added by data repo operations.  Do this before delete
        // resource from SAM to ensure we can get the metadata needed to perform the operation.  Also need to run
        // before metadata is deleted since it is required by the step.
        addStep(new DeleteDatasetAuthzBqAclsStep(iamClient, datasetService, resourceService, datasetId, userReq));
        addStep(new DeleteDatasetDeleteStorageAccountsStep(resourceService, datasetService, datasetId));
        addStep(new DeleteDatasetMetadataStep(datasetDao, datasetId));
        addStep(new DeleteDatasetAuthzResource(iamClient, datasetId, userReq));
        addStep(new UnlockDatasetStep(datasetDao, datasetId, false), lockDatasetRetry);
    }
}
