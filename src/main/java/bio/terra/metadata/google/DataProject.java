package bio.terra.metadata.google;

import java.util.UUID;

public class DataProject {

    private UUID repositoryId;
    private String googleProjectId;
    private String googleProjectNumber;
    private DataProjectRequest dataProjectRequest;

    public DataProject() {
        this.dataProjectRequest = new DataProjectRequest();
    }

    public DataProject(DataProjectRequest dataProjectRequest) {
        this.dataProjectRequest = dataProjectRequest;
    }

    public UUID getRepositoryId() {
        return repositoryId;
    }

    public DataProject repositoryId(UUID repositoryId) {
        this.repositoryId = repositoryId;
        return this;
    }

    public String getGoogleProjectId() {
        return googleProjectId;
    }

    public DataProject googleProjectId(String googleProjectId) {
        this.googleProjectId = googleProjectId;
        return this;
    }

    public String getGoogleProjectNumber() {
        return googleProjectNumber;
    }

    public DataProject googleProjectNumber(String googleProjectNumber) {
        this.googleProjectNumber = googleProjectNumber;
        return this;
    }

    public UUID getProfileId() {
        return dataProjectRequest.getProfileId();
    }

    public DataProject profileId(UUID profileId) {
        dataProjectRequest.profileId(profileId);
        return this;
    }

    public UUID getStudyId() {
        return dataProjectRequest.getStudyId();
    }

    public DataProject studyId(UUID studyId) {
        dataProjectRequest.studyId(studyId);
        return this;
    }

    public UUID getDatasetId() {
        return dataProjectRequest.getDatasetId();
    }

    public DataProject datasetId(UUID datasetId) {
        dataProjectRequest.datasetId(datasetId);
        return this;
    }
}
