package bio.terra.service.dataset.flight.datadelete;

import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class DataDeletionDeleteScratchFilesGcsStep implements Step {

  private final GcsPdao gcsPdao;

  public DataDeletionDeleteScratchFilesGcsStep(GcsPdao gcsPdao) {
    this.gcsPdao = gcsPdao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    DataDeletionUtils.deleteScratchFiles(context, gcsPdao);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
