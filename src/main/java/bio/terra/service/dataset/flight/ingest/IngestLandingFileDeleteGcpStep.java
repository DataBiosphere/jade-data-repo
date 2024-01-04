package bio.terra.service.dataset.flight.ingest;

import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;

public class IngestLandingFileDeleteGcpStep implements Step {

  private final boolean performInUndoPhase;
  private final GcsPdao gcsPdao;

  public IngestLandingFileDeleteGcpStep(boolean performInUndoPhase, GcsPdao gcsPdao) {
    this.performInUndoPhase = performInUndoPhase;
    this.gcsPdao = gcsPdao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    if (!performInUndoPhase) {
      IngestUtils.deleteLandingFile(context, gcsPdao);
      return StepResult.getStepResultSuccess();
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    if (performInUndoPhase) {
      IngestUtils.deleteLandingFile(context, gcsPdao);
      return StepResult.getStepResultSuccess();
    }
    return StepResult.getStepResultSuccess();
  }
}
