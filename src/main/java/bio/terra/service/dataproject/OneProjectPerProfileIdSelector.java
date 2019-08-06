package bio.terra.service.dataproject;

import bio.terra.metadata.BillingProfile;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.Snapshot;
import bio.terra.resourcemanagement.service.ProfileService;
import bio.terra.resourcemanagement.service.google.GoogleResourceConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@Profile("terra")
public class OneProjectPerProfileIdSelector implements DataProjectIdSelector {
    @Autowired
    private ProfileService profileService;

    private final GoogleResourceConfiguration resourceConfiguration;

    public OneProjectPerProfileIdSelector(GoogleResourceConfiguration resourceConfiguration) {
        this.resourceConfiguration = resourceConfiguration;
    }

    @Override
    public String projectIdForDataset(Dataset dataset) {
        return getSuffixForProfileId(dataset.getDefaultProfileId());
    }

    @Override
    public String projectIdForSnapshot(Snapshot snapshot) {
        return getSuffixForProfileId(snapshot.getProfileId());
    }

    private String getSuffixForProfileId(UUID profileId) {
        BillingProfile profile = profileService.getProfileById(profileId);
        String lowercaseProfileName = profile.getName().toLowerCase();
        String profileSuffix = "-" + lowercaseProfileName.replaceAll("[^a-z-]", "-");

        return resourceConfiguration.getProjectId() + profileSuffix;
    }
}
