package bio.terra.service.dataset.flight.upgrade.enableSecureMonitoring;

import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.common.JournalRecordCreateEntryStep;
import bio.terra.service.journal.JournalService;
import java.util.UUID;

public class EnableSecureMonitoringJournalEntryStep extends JournalRecordCreateEntryStep {
  private UUID datasetId;

  public EnableSecureMonitoringJournalEntryStep(
      UUID datasetId, JournalService journalService, AuthenticatedUserRequest userReq) {
    this.datasetId = datasetId;
    super(journalService, userReq, IamResourceType.DATASET, "Enabled secure monitoring", false);
  }

  @Override
  public UUID getResourceId(FlightContext context) throws InterruptedException {
    return datasetId;
  }
}
