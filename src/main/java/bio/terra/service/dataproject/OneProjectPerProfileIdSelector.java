package bio.terra.service.dataproject;

import bio.terra.metadata.BillingProfile;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.FSFile;
import bio.terra.metadata.Snapshot;
import bio.terra.resourcemanagement.service.ProfileService;
import bio.terra.resourcemanagement.service.google.GoogleResourceConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@Profile({"terra", "google"})
public class OneProjectPerProfileIdSelector implements DataLocationSelector {
    private final GoogleResourceConfiguration resourceConfiguration;
    private final ProfileService profileService;

    @Autowired
    public OneProjectPerProfileIdSelector(GoogleResourceConfiguration resourceConfiguration,
                                          ProfileService profileService) {
        this.resourceConfiguration = resourceConfiguration;
        this.profileService = profileService;
    }

    @Override
    public String projectIdForDataset(Dataset dataset) {
        return getSuffixForProfileId(dataset.getDefaultProfileId());
    }

    @Override
    public String projectIdForSnapshot(Snapshot snapshot) {
        return getSuffixForProfileId(snapshot.getProfileId());
    }

    @Override
    public String projectIdForFile(FSFile fsFile) {
        return getSuffixForProfileId(UUID.fromString(fsFile.getProfileId()));
    }

    @Override
    public String bucketForFile(FSFile fsFile) {
        return projectIdForFile(fsFile) + "-bucket";
    }

    private String getSuffixForProfileId(UUID profileId) {
        BillingProfile profile = profileService.getProfileById(profileId);
        String lowercaseProfileName = profile.getName().toLowerCase();
        String profileSuffix = "-" + lowercaseProfileName.replaceAll("[^a-z-0-9]", "-");

        return resourceConfiguration.getProjectId() + profileSuffix;
    }
}
