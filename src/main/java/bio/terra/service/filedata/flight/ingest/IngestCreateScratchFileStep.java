package bio.terra.service.filedata.flight.ingest;

import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class IngestCreateScratchFileStep implements Step {

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    GoogleBucketResource bucket = workingMap.get(FileMapKeys.INGEST_FILE_BUCKET_INFO,
        GoogleBucketResource.class);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
