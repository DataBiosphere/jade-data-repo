package bio.terra.service.filedata.flight.ingest;

import bio.terra.service.dataset.flight.ingest.IngestMapKeys;
import bio.terra.service.dataset.flight.ingest.SkippableStep;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.function.Predicate;

public class IngestCreateScratchFileStep extends SkippableStep {

  private final GcsPdao gcsPdao;

  public IngestCreateScratchFileStep(GcsPdao gcsPdao, Predicate<FlightContext> skipCondition) {
    super(skipCondition);
    this.gcsPdao = gcsPdao;
  }

  public IngestCreateScratchFileStep(GcsPdao gcsPdao) {
    this.gcsPdao = gcsPdao;
  }

  @Override
  public StepResult doSkippableStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    GoogleBucketResource bucket =
        workingMap.get(FileMapKeys.INGEST_FILE_BUCKET_INFO, GoogleBucketResource.class);
    List<String> linesWithFileIds = workingMap.get(IngestMapKeys.LINES_WITH_FILE_IDS, List.class);

    // this is a hackathon
    String path = "gs://" + bucket.getName() + "/" + context.getFlightId() + "-scratch.json";
    workingMap.put(IngestMapKeys.INGEST_SCRATCH_FILE_PATH, path);

    // new gs path:
    // gs://{bucketname}/{flightId}-scratch.json
    gcsPdao.writeGcsFileLines(path, linesWithFileIds, bucket);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoSkippableStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
