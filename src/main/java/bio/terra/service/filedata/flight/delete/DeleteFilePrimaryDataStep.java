package bio.terra.service.filedata.flight.delete;

import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class DeleteFilePrimaryDataStep implements Step {

  private final GcsPdao gcsPdao;

  public DeleteFilePrimaryDataStep(GcsPdao gcsPdao) {
    this.gcsPdao = gcsPdao;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    FireStoreFile fireStoreFile = workingMap.get(FileMapKeys.FIRESTORE_FILE, FireStoreFile.class);
    gcsPdao.deleteFile(fireStoreFile);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // No undo is possible - the file either still exists or it doesn't
    return StepResult.getStepResultSuccess();
  }
}
