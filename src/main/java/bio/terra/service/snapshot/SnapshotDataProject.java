package bio.terra.service.snapshot;

import bio.terra.service.resourcemanagement.google.GoogleProjectResource;

import java.util.List;
import java.util.UUID;

public class SnapshotDataProject {

    private SnapshotDataProjectSummary snapshotDataProjectSummary = new SnapshotDataProjectSummary();
    private GoogleProjectResource googleProjectResource = new GoogleProjectResource();

    public SnapshotDataProject() {}

    public SnapshotDataProject(SnapshotDataProjectSummary snapshotDataProjectSummary) {
        this.snapshotDataProjectSummary = snapshotDataProjectSummary;
    }

    public SnapshotDataProjectSummary getSnapshotDataProjectSummary() {
        return snapshotDataProjectSummary;
    }

    public SnapshotDataProject setSnapshotDataProjectSummary(SnapshotDataProjectSummary snapshotDataProjectSummary) {
        this.snapshotDataProjectSummary = snapshotDataProjectSummary;
        return this;
    }

    public GoogleProjectResource getGoogleProjectResource() {
        return googleProjectResource;
    }

    public SnapshotDataProject googleProjectResource(GoogleProjectResource projectResource) {
        this.googleProjectResource = projectResource;
        return this;
    }

    public String getGoogleProjectId() {
        return googleProjectResource.getGoogleProjectId();
    }

    public SnapshotDataProject googleProjectId(String googleProjectId) {
        googleProjectResource.googleProjectId(googleProjectId);
        return this;
    }

    public String getGoogleProjectNumber() {
        return googleProjectResource.getGoogleProjectNumber();
    }

    public SnapshotDataProject googleProjectNumber(String googleProjectNumber) {
        googleProjectResource.googleProjectNumber(googleProjectNumber);
        return this;
    }

    public UUID getProfileId() {
        return googleProjectResource.getProfileId();
    }

    public SnapshotDataProject profileId(UUID profileId) {
        googleProjectResource.profileId(profileId);
        return this;
    }

    public UUID getProjectResourceId() {
        return googleProjectResource.getRepositoryId();
    }

    public SnapshotDataProject projectResourceId(UUID projectResourceId) {
        googleProjectResource.repositoryId(projectResourceId);
        return this;
    }

    public List<String> getServiceIds() {
        return googleProjectResource.getServiceIds();
    }

    public SnapshotDataProject serviceIds(List<String> serviceIds) {
        googleProjectResource.serviceIds(serviceIds);
        return this;
    }

    public UUID getId() {
        return snapshotDataProjectSummary.getId();
    }

    public SnapshotDataProject id(UUID id) {
        snapshotDataProjectSummary.id(id);
        return this;
    }

    public UUID getSnapshotId() {
        return snapshotDataProjectSummary.getSnapshotId();
    }

    public SnapshotDataProject snapshotId(UUID snapshotId) {
        snapshotDataProjectSummary.snapshotId(snapshotId);
        return this;
    }
}

