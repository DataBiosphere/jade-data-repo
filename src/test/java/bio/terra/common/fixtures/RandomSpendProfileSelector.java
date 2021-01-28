package bio.terra.common.fixtures;

import bio.terra.model.BillingProfileModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Primary
@Profile("!terra")
public class RandomSpendProfileSelector implements SpendProfileSelector {
    @Autowired
    private ConnectedOperations connectedOperations;

    @Override
    public BillingProfileModel getOrCreateProfileForAccount(String billingAccountId) throws Exception {
        bio.terra.model.BillingProfileRequestModel profileRequestModel = ProfileFixtures.randomBillingProfileRequest()
            .billingAccountId(billingAccountId);
        return connectedOperations.createProfile(profileRequestModel);
    }
}
