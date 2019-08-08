package bio.terra.metadata;

import bio.terra.resourcemanagement.metadata.google.GoogleProjectResource;

import java.util.List;
import java.util.UUID;

public class FileDataProject {

    private FileDataProjectSummary fileDataProjectSummary = new FileDataProjectSummary();
    private GoogleProjectResource googleProjectResource = new GoogleProjectResource();

    public FileDataProject() {}

    public FileDataProject(FileDataProjectSummary fileDataProjectSummary) {
        this.fileDataProjectSummary = fileDataProjectSummary;
    }

    public FileDataProjectSummary getFileDataProjectSummary() {
        return fileDataProjectSummary;
    }

    public FileDataProject setFileDataProjectSummary(FileDataProjectSummary fileDataProjectSummary) {
        this.fileDataProjectSummary = fileDataProjectSummary;
        return this;
    }

    public GoogleProjectResource getGoogleProjectResource() {
        return googleProjectResource;
    }

    public FileDataProject googleProjectResource(GoogleProjectResource projectResource) {
        this.googleProjectResource = projectResource;
        return this;
    }

    public String getGoogleProjectId() {
        return googleProjectResource.getGoogleProjectId();
    }

    public FileDataProject googleProjectId(String googleProjectId) {
        googleProjectResource.googleProjectId(googleProjectId);
        return this;
    }

    public String getGoogleProjectNumber() {
        return googleProjectResource.getGoogleProjectNumber();
    }

    public FileDataProject googleProjectNumber(String googleProjectNumber) {
        googleProjectResource.googleProjectNumber(googleProjectNumber);
        return this;
    }

    public UUID getProfileObjectId() {
        return googleProjectResource.getProfileId();
    }

    public FileDataProject profileId(UUID profileId) {
        googleProjectResource.profileId(profileId);
        return this;
    }

    public UUID getProjectResourceId() {
        return googleProjectResource.getRepositoryId();
    }

    public FileDataProject projectResourceId(UUID projectResourceId) {
        googleProjectResource.repositoryId(projectResourceId);
        return this;
    }

    public List<String> getServiceIds() {
        return googleProjectResource.getServiceIds();
    }

    public FileDataProject serviceIds(List<String> serviceIds) {
        googleProjectResource.serviceIds(serviceIds);
        return this;
    }

    public UUID getId() {
        return fileDataProjectSummary.getId();
    }

    public FileDataProject id(UUID id) {
        fileDataProjectSummary.id(id);
        return this;
    }

    public UUID getFileObjectId() {
        return fileDataProjectSummary.getFileObjectId();
    }

    public FileDataProject fileObjectId(UUID fileObjectId) {
        fileDataProjectSummary.fileObjectId(fileObjectId);
        return this;
    }
}
