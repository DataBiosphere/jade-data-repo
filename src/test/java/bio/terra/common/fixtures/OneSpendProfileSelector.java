package bio.terra.common.fixtures;

import bio.terra.model.BillingProfileModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile({"terra", "google"})
public class OneSpendProfileSelector implements SpendProfileSelector {
    @Autowired
    private ConnectedOperations connectedOperations;

    @Override
    public BillingProfileModel getOrCreateProfileForAccount(String billingAccountId) throws Exception {
        //TODO Get Spend profile for Data project
        return new BillingProfileModel();
    }
}
