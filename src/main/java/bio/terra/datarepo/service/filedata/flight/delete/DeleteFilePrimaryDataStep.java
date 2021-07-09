package bio.terra.datarepo.service.filedata.flight.delete;

import bio.terra.datarepo.service.filedata.flight.FileMapKeys;
import bio.terra.datarepo.service.filedata.google.firestore.FireStoreFile;
import bio.terra.datarepo.service.filedata.google.gcs.GcsPdao;
import bio.terra.datarepo.service.resourcemanagement.ResourceService;
import bio.terra.datarepo.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteFilePrimaryDataStep implements Step {
  private static final Logger logger = LoggerFactory.getLogger(DeleteFilePrimaryDataStep.class);

  private final GcsPdao gcsPdao;
  private final ResourceService resourceService;

  public DeleteFilePrimaryDataStep(GcsPdao gcsPdao, ResourceService resourceService) {
    this.gcsPdao = gcsPdao;
    this.resourceService = resourceService;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    FireStoreFile fireStoreFile = workingMap.get(FileMapKeys.FIRESTORE_FILE, FireStoreFile.class);
    if (fireStoreFile != null) {
      GoogleBucketResource bucketResource =
          resourceService.lookupBucket(fireStoreFile.getBucketResourceId());
      gcsPdao.deleteFileByGspath(fireStoreFile.getGspath(), bucketResource);
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // No undo is possible - the file either still exists or it doesn't
    return StepResult.getStepResultSuccess();
  }
}
