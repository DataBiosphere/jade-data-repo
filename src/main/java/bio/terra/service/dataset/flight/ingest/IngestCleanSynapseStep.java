package bio.terra.service.dataset.flight.ingest;

import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.List;

public class IngestCleanSynapseStep implements Step {
  private AzureSynapsePdao azureSynapsePdao;

  public IngestCleanSynapseStep(AzureSynapsePdao azureSynapsePdao) {
    this.azureSynapsePdao = azureSynapsePdao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    azureSynapsePdao.dropTables(
        List.of(
            IngestUtils.getSynapseScratchTableName(context.getFlightId()),
            IngestUtils.getSynapseIngestTableName(context.getFlightId())));
    azureSynapsePdao.dropDataSources(
        List.of(
            IngestUtils.getScratchDataSourceName(context.getFlightId()),
            IngestUtils.getTargetDataSourceName(context.getFlightId()),
            IngestUtils.getIngestRequestDataSourceName(context.getFlightId())));
    azureSynapsePdao.dropScopedCredentials(
        List.of(
            IngestUtils.getScratchScopedCredentialName(context.getFlightId()),
            IngestUtils.getTargetScopedCredentialName(context.getFlightId()),
            IngestUtils.getIngestRequestScopedCredentialName(context.getFlightId())));

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
