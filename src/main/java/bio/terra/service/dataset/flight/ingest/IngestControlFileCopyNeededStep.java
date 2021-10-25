package bio.terra.service.dataset.flight.ingest;

import bio.terra.app.model.CloudRegion;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestControlFileCopyNeededStep implements Step {

  private Logger logger = LoggerFactory.getLogger(IngestControlFileCopyNeededStep.class);

  private final DatasetService datasetService;
  private final GcsPdao gcsPdao;

  public IngestControlFileCopyNeededStep(DatasetService datasetService, GcsPdao gcsPdao) {
    this.datasetService = datasetService;
    this.gcsPdao = gcsPdao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    IngestRequestModel ingestRequest = IngestUtils.getIngestRequestModel(context);
    String pathToIngestFile = ingestRequest.getPath();
    if (IngestUtils.isCombinedFileIngest(context)) {
      // Files were ingested and we've already written out a control file in the scratch bucket.
      workingMap.put(IngestMapKeys.CONTROL_FILE_NEEDS_COPY, false);
    } else {
      GoogleRegion regionForFile =
          gcsPdao.getRegionForFile(
              pathToIngestFile, dataset.getProjectResource().getGoogleProjectId());
      CloudRegion bigQueryRegion =
          dataset.getDatasetSummary().getStorageResourceRegion(GoogleCloudResource.BIGQUERY);
      boolean controlFileNeedsCopy = bigQueryRegion != regionForFile;
      workingMap.put(IngestMapKeys.CONTROL_FILE_NEEDS_COPY, controlFileNeedsCopy);
      if (controlFileNeedsCopy) {
        logger.info(
            "Control file and BigQuery are not in the same region. "
                + "Copying control file to a scratch bucket in the correct region.");
      } else {
        // Put the request's control file path into the working map since it doesn't need to be
        // copied
        workingMap.put(IngestMapKeys.INGEST_CONTROL_FILE_PATH, pathToIngestFile);
      }
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
