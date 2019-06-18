package bio.terra.metadata.google;

import java.util.UUID;

public class DataProjectRequest {

    private UUID profileId;
    private UUID studyId;
    private UUID datasetId;

    public UUID getProfileId() {
        return profileId;
    }

    public DataProjectRequest profileId(UUID profileId) {
        this.profileId = profileId;
        return this;
    }

    public UUID getStudyId() {
        return studyId;
    }

    public DataProjectRequest studyId(UUID studyId) {
        this.studyId = studyId;
        return this;
    }

    public UUID getDatasetId() {
        return datasetId;
    }

    public DataProjectRequest datasetId(UUID datasetId) {
        this.datasetId = datasetId;
        return this;
    }
}
