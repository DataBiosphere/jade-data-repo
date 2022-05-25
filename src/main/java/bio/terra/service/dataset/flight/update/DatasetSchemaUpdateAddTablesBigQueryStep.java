package bio.terra.service.dataset.flight.update;

import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.model.DatasetSpecificationModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetJsonConversion;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.tabulardata.google.BigQueryProject;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.service.tabulardata.google.bigquery.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import com.google.cloud.bigquery.BigQuery;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

public class DatasetSchemaUpdateAddTablesBigQueryStep implements Step {

  private static final Logger logger =
      LoggerFactory.getLogger(DatasetSchemaUpdateAddTablesBigQueryStep.class);

  private final BigQueryDatasetPdao bigQueryDatasetPdao;
  private final DatasetDao datasetDao;
  private final UUID datasetId;

  private final DatasetSchemaUpdateModel updateModel;

  public DatasetSchemaUpdateAddTablesBigQueryStep(
      BigQueryDatasetPdao bigQueryDatasetPdao,
      DatasetDao datasetDao,
      UUID datasetId,
      DatasetSchemaUpdateModel updateModel) {
    this.bigQueryDatasetPdao = bigQueryDatasetPdao;
    this.datasetDao = datasetDao;
    this.datasetId = datasetId;
    this.updateModel = updateModel;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    Dataset dataset = datasetDao.retrieve(datasetId);
    List<String> newTableNames = DatasetSchemaUpdateUtils.getNewTableNames(updateModel);
    List<DatasetTable> tables =
        dataset.getTables().stream()
            .filter(dt -> newTableNames.contains(dt.getName()))
            .collect(Collectors.toList());

    BigQueryProject bigQueryProject = BigQueryProject.from(dataset);
    BigQuery bigQuery = bigQueryProject.getBigQuery();
    String datasetName = BigQueryPdao.prefixName(dataset.getName());

    for (DatasetTable table : tables) {
      bigQueryDatasetPdao.createTable(bigQueryProject, bigQuery, datasetName, table);
    }

    DatasetSpecificationModel updatedSchema =
        DatasetJsonConversion.datasetSpecificationModelFromDatasetSchema(dataset);
    FlightMap workingMap = context.getWorkingMap();
    workingMap.put(JobMapKeys.RESPONSE.getKeyName(), updatedSchema);
    workingMap.put(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.OK);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    Dataset dataset = datasetDao.retrieve(datasetId);
    List<String> newTableNames = DatasetSchemaUpdateUtils.getNewTableNames(updateModel);
    for (String tableName : newTableNames) {
      Optional<DatasetTable> table = dataset.getTableByName(tableName);
      if (table.isPresent()) {
        bigQueryDatasetPdao.undoDatasetTableCreate(dataset, table.get());
      } else {
        logger.warn(
            "Could not delete nonexistent table '{}' from dataset '{}' in BigQuery",
            tableName,
            dataset.getName());
      }
    }
    return StepResult.getStepResultSuccess();
  }
}
