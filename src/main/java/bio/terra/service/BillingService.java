package bio.terra.service;

import bio.terra.metadata.BillingProfile;

public interface BillingService {

    boolean canAccess(BillingProfile billingProfile);

}
