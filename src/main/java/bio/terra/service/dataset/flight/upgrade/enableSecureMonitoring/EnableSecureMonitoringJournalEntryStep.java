package bio.terra.service.dataset.flight.upgrade.enableSecureMonitoring;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.common.JournalRecordCreateEntryStep;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.journal.JournalService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import java.util.UUID;

public class EnableSecureMonitoringJournalEntryStep extends JournalRecordCreateEntryStep {
  public EnableSecureMonitoringJournalEntryStep(
      JournalService journalService, AuthenticatedUserRequest userReq) {
    super(journalService, userReq, IamResourceType.DATASET, "Enabled secure monitoring", false);
  }

  @Override
  public UUID getResourceId(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    return workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
  }
}
