package bio.terra.service.dataset.flight.update;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.common.JournalRecordUpdateEntryStep;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetRelationshipDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTableDao;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.journal.JournalService;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class DatasetSchemaUpdateFlight extends Flight {

  public DatasetSchemaUpdateFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    ApplicationContext appContext = (ApplicationContext) applicationContext;
    DatasetDao datasetDao = appContext.getBean(DatasetDao.class);
    DatasetTableDao datasetTableDao = appContext.getBean(DatasetTableDao.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    DatasetRelationshipDao relationshipDao = appContext.getBean(DatasetRelationshipDao.class);
    BigQueryDatasetPdao bigQueryDatasetPdao = appContext.getBean(BigQueryDatasetPdao.class);
    JournalService journalService = appContext.getBean(JournalService.class);

    AuthenticatedUserRequest userRequest =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    DatasetSchemaUpdateModel updateModel =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), DatasetSchemaUpdateModel.class);

    UUID datasetId =
        UUID.fromString(inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class));

    Dataset dataset = datasetDao.retrieve(datasetId);
    CloudPlatformWrapper cloudPlatform =
        CloudPlatformWrapper.of(dataset.getDatasetSummary().getCloudPlatform());

    addStep(new LockDatasetStep(datasetService, datasetId, false));

    addStep(new DatasetSchemaUpdateValidateModelStep(datasetService, datasetId, updateModel));

    if (DatasetSchemaUpdateUtils.hasTableAdditions(updateModel)) {
      addStep(
          new DatasetSchemaUpdateAddTablesPostgresStep(datasetTableDao, datasetId, updateModel));
      if (cloudPlatform.isGcp()) {
        addStep(
            new DatasetSchemaUpdateAddTablesBigQueryStep(
                bigQueryDatasetPdao, datasetDao, datasetId, updateModel));
      }
    }

    if (DatasetSchemaUpdateUtils.hasColumnAdditions(updateModel)) {
      addStep(
          new DatasetSchemaUpdateAddColumnsPostgresStep(datasetTableDao, datasetId, updateModel));
      if (cloudPlatform.isGcp()) {
        addStep(
            new DatasetSchemaUpdateAddColumnsBigQueryStep(
                bigQueryDatasetPdao, datasetDao, datasetId, updateModel));
      }
    }

    if (DatasetSchemaUpdateUtils.hasRelationshipAdditions(updateModel)) {
      addStep(
          new DatasetSchemaUpdateAddRelationshipsPostgresStep(
              datasetDao, datasetTableDao, datasetId, relationshipDao, updateModel));
    }

    addStep(new UnlockDatasetStep(datasetService, datasetId, false));
    addStep(
        new DatasetSchemaUpdateResponseStep(
            datasetDao, datasetId, datasetService, updateModel, userRequest));

    addStep(
        new JournalRecordUpdateEntryStep(
            journalService, userRequest, datasetId, IamResourceType.DATASET, "Schema updated."));
  }
}
