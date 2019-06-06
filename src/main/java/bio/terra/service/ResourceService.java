package bio.terra.service;

import bio.terra.dao.ResourceDao;
import bio.terra.metadata.BillingProfile;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class ResourceService {

    private final ResourceDao resourceDao;
    private final BillingService billingService;

    @Autowired
    public ResourceService(ResourceDao resourceDao, BillingService billingService) {
        this.resourceDao = resourceDao;
        this.billingService = billingService;
    }

    public BillingProfileModel makeModelFromBillingProfile(BillingProfile billingProfile) {
        return new BillingProfileModel()
            .id(billingProfile.getId().toString())
            .profileName(billingProfile.getName())
            .biller(billingProfile.getBiller())
            .billingAccountId(billingProfile.getBillingAccountId())
            .accessible(billingProfile.isAccessible());
    }

    public BillingProfileModel createProfile(BillingProfileRequestModel billingProfileRequest) {
        BillingProfile profile = new BillingProfile()
            .name(billingProfileRequest.getProfileName())
            .biller(billingProfileRequest.getBiller())
            .billingAccountId(billingProfileRequest.getBillingAccountId());
        UUID profileId = resourceDao.createBillingProfile(profile);
        profile
            .id(profileId)
            .accessible(billingService.canAccess(profile));
        return makeModelFromBillingProfile(profile);
    }
}
