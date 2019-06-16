package bio.terra.metadata.google;

import java.util.UUID;

public class GoogleProject {

    private UUID repositoryId;
    private String googleProjectId;
    private UUID profileId;
    private UUID studyId;

    public UUID getRepositoryId() {
        return repositoryId;
    }

    public GoogleProject repositoryId(UUID repositoryId) {
        this.repositoryId = repositoryId;
        return this;
    }

    public String getGoogleProjectId() {
        return googleProjectId;
    }

    public GoogleProject googleProjectId(String googleProjectId) {
        this.googleProjectId = googleProjectId;
        return this;
    }

    public UUID getProfileId() {
        return profileId;
    }

    public GoogleProject profileId(UUID profileId) {
        this.profileId = profileId;
        return this;
    }

    public UUID getStudyId() {
        return studyId;
    }

    public GoogleProject studyId(UUID studyId) {
        this.studyId = studyId;
        return this;
    }
}
