package bio.terra.service.snapshot.flight.create;

import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.Arrays;

public class CreateSnapshotCleanSynapseAzureStep implements Step {
  private AzureSynapsePdao azureSynapsePdao;

  public CreateSnapshotCleanSynapseAzureStep(AzureSynapsePdao azureSynapsePdao) {
    this.azureSynapsePdao = azureSynapsePdao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    // TODO - switch to reference snapshot sources instead of ingest
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
