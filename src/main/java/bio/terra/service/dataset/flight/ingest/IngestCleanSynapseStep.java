package bio.terra.service.dataset.flight.ingest;

import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.Arrays;

public class IngestCleanSynapseStep implements Step {
  private AzureSynapsePdao azureSynapsePdao;

  public IngestCleanSynapseStep(AzureSynapsePdao azureSynapsePdao) {
    this.azureSynapsePdao = azureSynapsePdao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    azureSynapsePdao.dropTables(
        Arrays.asList(IngestUtils.getSynapseTableName(context.getFlightId())));
    azureSynapsePdao.dropDataSources(
        Arrays.asList(
            IngestUtils.getTargetDataSourceName(context.getFlightId()),
            IngestUtils.getIngestRequestDataSourceName(context.getFlightId())));
    azureSynapsePdao.dropScopedCredentials(
        Arrays.asList(
            IngestUtils.getTargetScopedCredentialName(context.getFlightId()),
            IngestUtils.getIngestRequestScopedCredentialName(context.getFlightId())));

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
