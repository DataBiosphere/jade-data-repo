package bio.terra.service;

import bio.terra.dao.ProfileDao;
import bio.terra.metadata.BillingProfile;
import bio.terra.metadata.MetadataEnumeration;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.EnumerateBillingProfileModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Component
public class ProfileService {

    private final ProfileDao profileDao;
    private final BillingService billingService;

    @Autowired
    public ProfileService(ProfileDao profileDao, BillingService billingService) {
        this.profileDao = profileDao;
        this.billingService = billingService;
    }

    public static BillingProfileModel makeModelFromBillingProfile(BillingProfile billingProfile) {
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
        UUID profileId = profileDao.createBillingProfile(profile);
        profile.id(profileId);
        updateAccessibility(profile);
        return makeModelFromBillingProfile(profile);
    }

    public EnumerateBillingProfileModel enumerateProfiles(Integer offset, Integer limit) {
        MetadataEnumeration<BillingProfile> profileEnumeration = profileDao.enumerateBillingProfiles(offset, limit);
        List<BillingProfileModel> profileModels = profileEnumeration.getItems()
            .stream()
            .map(this::updateAccessibility)
            .map(ProfileService::makeModelFromBillingProfile)
            .collect(Collectors.toList());
        return new EnumerateBillingProfileModel()
            .items(profileModels)
            .total(profileEnumeration.getTotal());
    }

    public BillingProfile getProfileById(UUID id) {
        BillingProfile profile = profileDao.getBillingProfileById(id);
        updateAccessibility(profile);
        return profile;
    }

    private BillingProfile updateAccessibility(BillingProfile billingProfile) {
        return billingProfile.accessible(billingService.canAccess(billingProfile));
    }

    public DeleteResponseModel deleteProfileById(UUID id) {
        // TODO: ensure there aren't dependencies
        boolean deleted = profileDao.deleteBillingProfileById(id);
        return new DeleteResponseModel().objectState(deleted ? DeleteResponseModel.ObjectStateEnum.DELETED :
            DeleteResponseModel.ObjectStateEnum.NOT_FOUND);
    }
}
