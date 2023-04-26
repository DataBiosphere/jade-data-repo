package bio.terra.service.profile.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.common.JournalRecordCreateEntryStep;
import bio.terra.service.journal.JournalService;
import bio.terra.stairway.FlightContext;
import java.util.UUID;

public class CreateProfileJournalEntryStep extends JournalRecordCreateEntryStep {
  private final BillingProfileRequestModel billingReq;

  public CreateProfileJournalEntryStep(
      JournalService journalService,
      AuthenticatedUserRequest userReq,
      IamResourceType resourceType,
      String note,
      boolean clearHistory,
      BillingProfileRequestModel request) {
    super(journalService, userReq, resourceType, note, clearHistory);
    this.billingReq = request;
  }

  @Override
  public UUID getResourceId(FlightContext context) throws InterruptedException {
    return billingReq.getId();
  }
}
