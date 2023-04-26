package bio.terra.service.dataset.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.common.JournalRecordCreateEntryStep;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.journal.JournalService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import java.util.UUID;

public class CreateDatasetJournalEntryStep extends JournalRecordCreateEntryStep {

  public CreateDatasetJournalEntryStep(
      JournalService journalService,
      AuthenticatedUserRequest userReq,
      IamResourceType resourceType,
      String note,
      boolean clearHistory) {
    super(journalService, userReq, resourceType, note, clearHistory);
  }

  @Override
  public UUID getResourceId(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    return workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
  }
}
