package bio.terra.common.fixtures;

import bio.terra.model.BillingProfileModel;


public interface SpendProfileSelector {
    BillingProfileModel getOrCreateProfileForAccount(String billingAccountId) throws Exception;
}
