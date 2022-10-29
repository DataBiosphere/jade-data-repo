package bio.terra.service.dataset.flight.create;

import static bio.terra.service.common.CommonFlightUtils.getFlightInformationOfInterest;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.journal.JournalService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.Optional;
import java.util.UUID;

public class CreateDatasetJournalEntryStep implements Step {
  private final AuthenticatedUserRequest userReq;
  JournalService journalService;
  private UUID journalEntryKey;

  public CreateDatasetJournalEntryStep(
      JournalService journalService, AuthenticatedUserRequest userReq) {
    this.journalService = journalService;
    this.userReq = userReq;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    UUID datasetUUID = workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
    this.journalEntryKey =
        journalService.journalCreate(
            userReq,
            datasetUUID,
            IamResourceType.DATASET,
            "Created dataset.",
            getFlightInformationOfInterest(context));
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    Optional.ofNullable(this.journalEntryKey).ifPresent(this.journalService::removeJournalEntry);
    return StepResult.getStepResultSuccess();
  }
}
