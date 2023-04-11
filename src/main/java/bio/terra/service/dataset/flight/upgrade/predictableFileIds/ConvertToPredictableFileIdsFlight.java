package bio.terra.service.dataset.flight.upgrade.predictableFileIds;

import static bio.terra.common.FlightUtils.getDefaultRandomBackoffRetryRule;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.common.JournalRecordUpdateEntryStep;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.dataset.flight.transactions.TransactionCommitStep;
import bio.terra.service.dataset.flight.transactions.TransactionOpenStep;
import bio.terra.service.filedata.FileIdService;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.gcs.GcsProjectFactory;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.journal.JournalService;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.service.tabulardata.google.bigquery.BigQueryTransactionPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRuleRandomBackoff;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class ConvertToPredictableFileIdsFlight extends Flight {

  public ConvertToPredictableFileIdsFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    // get the required daos to pass into the steps
    ApplicationContext appContext = (ApplicationContext) applicationContext;

    ApplicationConfiguration appConfig = appContext.getBean(ApplicationConfiguration.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    SnapshotService snapshotService = appContext.getBean(SnapshotService.class);
    FireStoreDao fileDao = appContext.getBean(FireStoreDao.class);
    FileIdService fileIdService = appContext.getBean(FileIdService.class);
    BigQueryTransactionPdao bigQueryTransactionPdao =
        appContext.getBean(BigQueryTransactionPdao.class);
    BigQueryDatasetPdao bigQueryDatasetPdao = appContext.getBean(BigQueryDatasetPdao.class);
    GcsProjectFactory gcsProjectFactory = appContext.getBean(GcsProjectFactory.class);
    JournalService journalService = appContext.getBean(JournalService.class);
    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    UUID datasetId = inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), UUID.class);

    RetryRuleRandomBackoff retryRule =
        getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads());
    addStep(new LockDatasetStep(datasetService, datasetId, false));

    // Verify dataset can be upgraded
    addStep(new ConvertToPredictableFileIdsVerifyDatasetStep(datasetId, snapshotService, userReq));

    // Firestore steps
    // Side effect: add any missing MD5s values to file references in Firestore
    addStep(
        new ConvertToPredictableFileIdsUpdateMissingMd5ChecksumsStep(
            datasetId, datasetService, fileDao, gcsProjectFactory));
    addStep(
        new ConvertToPredictableFileIdsGetIdsStep(
            datasetId, datasetService, fileDao, fileIdService),
        retryRule);
    addStep(
        new ConvertToPredictableFileIdsUpdateFirestoreCollectionsStep(
            datasetId, datasetService, fileDao),
        retryRule);

    // BQ steps
    String transactionDesc = "Autocommit transaction";
    addStep(
        new TransactionOpenStep(
            datasetService, bigQueryTransactionPdao, userReq, transactionDesc, false, false),
        retryRule);
    addStep(
        new ConvertToPredictableFileIdsBqCreateStageTableStep(
            datasetId, datasetService, bigQueryDatasetPdao));
    addStep(
        new ConvertToPredictableFileIdsBqStageDataStep(
            datasetId, datasetService, bigQueryDatasetPdao),
        retryRule);
    addStep(
        new ConvertToPredictableFileIdsBqUpdateRowsStep(
            datasetId, datasetService, bigQueryDatasetPdao, userReq),
        retryRule);
    addStep(
        new TransactionCommitStep(datasetService, bigQueryTransactionPdao, userReq, false, null),
        retryRule);

    // Update metadata
    addStep(new ConvertToPredictableFileIdsUpdateMetadataStep(datasetId, datasetService));

    // Record log message
    addStep(new ConvertToPredictableFileIdsSetResponseStep(datasetId, datasetService));

    // Cleanup steps
    addStep(
        new ConvertToPredictableFileIdsBqDropStageTableStep(
            datasetId, datasetService, bigQueryDatasetPdao));

    addStep(
        new JournalRecordUpdateEntryStep(
            journalService,
            userReq,
            datasetId,
            IamResourceType.DATASET,
            "FileIds converted to predictable ids on dataset."));
    addStep(new UnlockDatasetStep(datasetService, datasetId, false));
  }
}
