package bio.terra.service.dataset;

import bio.terra.service.resourcemanagement.google.GoogleProjectResource;

import java.util.List;
import java.util.UUID;

public class DatasetDataProject {

    private DatasetDataProjectSummary datasetDataProjectSummary = new DatasetDataProjectSummary();
    private GoogleProjectResource googleProjectResource = new GoogleProjectResource();

    public DatasetDataProject() {}

    public DatasetDataProject(DatasetDataProjectSummary datasetDataProjectSummary) {
        this.datasetDataProjectSummary = datasetDataProjectSummary;
    }

    public DatasetDataProjectSummary getDatasetDataProjectSummary() {
        return datasetDataProjectSummary;
    }

    public DatasetDataProject setDatasetDataProjectSummary(DatasetDataProjectSummary datasetDataProjectSummary) {
        this.datasetDataProjectSummary = datasetDataProjectSummary;
        return this;
    }

    public GoogleProjectResource getGoogleProjectResource() {
        return googleProjectResource;
    }

    public DatasetDataProject googleProjectResource(GoogleProjectResource projectResource) {
        this.googleProjectResource = projectResource;
        return this;
    }

    public String getGoogleProjectId() {
        return googleProjectResource.getGoogleProjectId();
    }

    public DatasetDataProject googleProjectId(String googleProjectId) {
        googleProjectResource.googleProjectId(googleProjectId);
        return this;
    }

    public String getGoogleProjectNumber() {
        return googleProjectResource.getGoogleProjectNumber();
    }

    public DatasetDataProject googleProjectNumber(String googleProjectNumber) {
        googleProjectResource.googleProjectNumber(googleProjectNumber);
        return this;
    }

    public UUID getProfileId() {
        return googleProjectResource.getProfileId();
    }

    public DatasetDataProject profileId(UUID profileId) {
        googleProjectResource.profileId(profileId);
        return this;
    }

    public UUID getProjectResourceId() {
        return googleProjectResource.getRepositoryId();
    }

    public DatasetDataProject projectResourceId(UUID projectResourceId) {
        googleProjectResource.repositoryId(projectResourceId);
        return this;
    }

    public List<String> getServiceIds() {
        return googleProjectResource.getServiceIds();
    }

    public DatasetDataProject serviceIds(List<String> serviceIds) {
        googleProjectResource.serviceIds(serviceIds);
        return this;
    }

    public UUID getId() {
        return datasetDataProjectSummary.getId();
    }

    public DatasetDataProject id(UUID id) {
        datasetDataProjectSummary.id(id);
        return this;
    }

    public UUID getDatasetId() {
        return datasetDataProjectSummary.getDatasetId();
    }

    public DatasetDataProject datasetId(UUID datasetId) {
        datasetDataProjectSummary.datasetId(datasetId);
        return this;
    }
}
