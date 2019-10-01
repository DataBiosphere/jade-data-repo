package bio.terra.resourcemanagement;

import bio.terra.metadata.BillingProfile;

public interface BillingService {

    boolean canAccess(BillingProfile billingProfile);

}
