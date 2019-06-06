package bio.terra.service;

import bio.terra.metadata.BillingProfile;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Service
@Profile("google")
public class GoogleBillingService implements BillingService {

    @Override
    public boolean canAccess(BillingProfile billingProfile) {
        return false;
    }
}
