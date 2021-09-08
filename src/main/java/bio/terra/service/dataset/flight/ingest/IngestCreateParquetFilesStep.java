package bio.terra.service.dataset.flight.ingest;

import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.azure.storage.blob.BlobUrlParts;
import java.sql.SQLException;
import java.util.Arrays;

public class IngestCreateParquetFilesStep implements Step {

  private AzureSynapsePdao azureSynapsePdao;
  private DatasetService datasetService;

  public IngestCreateParquetFilesStep(
      AzureSynapsePdao azureSynapsePdao, DatasetService datasetService) {
    this.azureSynapsePdao = azureSynapsePdao;
    this.datasetService = datasetService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    IngestRequestModel ingestRequestModel = IngestUtils.getIngestRequestModel(context);
    FlightMap workingMap = context.getWorkingMap();
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    DatasetTable targetTable = IngestUtils.getDatasetTable(context, dataset);
    String parquetFilePath = workingMap.get(IngestMapKeys.PARQUET_FILE_PATH, String.class);
    BlobUrlParts ingestRequestUrlBlob = BlobUrlParts.parse(ingestRequestModel.getPath());

    long updateCount;
    try {
      updateCount =
          azureSynapsePdao.createParquetFiles(
              ingestRequestModel.getFormat(),
              targetTable,
              ingestRequestUrlBlob.getBlobName(),
              parquetFilePath,
              IngestUtils.getTargetDataSourceName(context.getFlightId()),
              IngestUtils.getIngestRequestDataSourceName(context.getFlightId()),
              IngestUtils.getSynapseTableName(context.getFlightId()),
              ingestRequestModel.getCsvSkipLeadingRows(),
              ingestRequestModel.getCsvFieldDelimiter(),
              ingestRequestModel.getCsvQuote());

    } catch (SQLException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }

    IngestRequestModel ingestRequest = IngestUtils.getIngestRequestModel(context);
    IngestResponseModel ingestResponse =
        new IngestResponseModel()
            .dataset(dataset.getName())
            .datasetId(dataset.getId())
            .table(ingestRequest.getTable())
            .path(ingestRequest.getPath())
            .loadTag(ingestRequest.getLoadTag())
            .badRowCount(0L) // TODO - determine values w/ DR-2016
            .rowCount(updateCount);
    context.getWorkingMap().put(JobMapKeys.RESPONSE.getKeyName(), ingestResponse);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    azureSynapsePdao.dropTables(
        Arrays.asList(IngestUtils.getIngestRequestDataSourceName(context.getFlightId())));
    return StepResult.getStepResultSuccess();
  }
}
