package bio.terra.service.common;

import static bio.terra.service.common.CommonFlightUtils.getFlightInformationOfInterest;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.journal.JournalService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.Optional;
import java.util.UUID;

public class JournalRecordDeleteEntryStep implements Step {
  private final JournalService journalService;
  private final AuthenticatedUserRequest userReq;
  private final UUID resourceKey;
  private final IamResourceType resourceType;
  private final String note;

  public JournalRecordDeleteEntryStep(
      JournalService journalService,
      AuthenticatedUserRequest userRequest,
      UUID resourceKey,
      IamResourceType resourceType,
      String note) {
    this.journalService = journalService;
    this.userReq = userRequest;
    this.resourceKey = resourceKey;
    this.resourceType = resourceType;
    this.note = note;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    UUID journalEntryKey =
        journalService.recordDelete(
            userReq, resourceKey, resourceType, note, getFlightInformationOfInterest(context));
    workingMap.put(JobMapKeys.JOURNAL_ENTRY_KEY.getKeyName(), journalEntryKey);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    UUID journalEntryKey = workingMap.get(JobMapKeys.JOURNAL_ENTRY_KEY.getKeyName(), UUID.class);
    Optional.ofNullable(journalEntryKey).ifPresent(journalService::removeJournalEntry);
    return StepResult.getStepResultSuccess();
  }
}
