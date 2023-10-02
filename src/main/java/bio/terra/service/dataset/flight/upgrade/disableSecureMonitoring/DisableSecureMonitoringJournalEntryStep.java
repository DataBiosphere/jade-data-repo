package bio.terra.service.dataset.flight.upgrade.disableSecureMonitoring;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.common.JournalRecordCreateEntryStep;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.journal.JournalService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import java.util.UUID;

public class DisableSecureMonitoringJournalEntryStep extends JournalRecordCreateEntryStep {
  public DisableSecureMonitoringJournalEntryStep(
      JournalService journalService, AuthenticatedUserRequest userReq) {
    super(journalService, userReq, IamResourceType.DATASET, "Disable secure monitoring", false);
  }

  @Override
  public UUID getResourceId(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    return workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
  }
}
