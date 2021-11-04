package bio.terra.service.dataset.flight.ingest;

import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;

public class IngestScratchFileDeleteGcpStep implements Step {

  private final GcsPdao gcsPdao;

  public IngestScratchFileDeleteGcpStep(GcsPdao gcsPdao) {

    this.gcsPdao = gcsPdao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    IngestUtils.deleteScratchFile(context, gcsPdao);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
