package bio.terra.service;

import bio.terra.dao.ResourceDao;
import bio.terra.metadata.BillingProfile;
import bio.terra.metadata.MetadataEnumeration;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.EnumerateBillingProfileModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

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

    public EnumerateBillingProfileModel enumerateProfiles(Integer offset, Integer limit) {
        MetadataEnumeration<BillingProfile> profileEnumeration = resourceDao.enumerateBillingProfiles(offset, limit);
        List<BillingProfileModel> profileModels = profileEnumeration.getItems()
            .stream()
            .map(this::makeModelFromBillingProfile)
            .collect(Collectors.toList());
        return new EnumerateBillingProfileModel()
            .items(profileModels)
            .total(profileEnumeration.getTotal());
    }
}
