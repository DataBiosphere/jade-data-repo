package bio.terra.metadata;

import bio.terra.resourcemanagement.metadata.google.GoogleProjectResource;

import java.util.List;
import java.util.UUID;

public class StudyDataProject {

    private StudyDataProjectSummary studyDataProjectSummary = new StudyDataProjectSummary();
    private GoogleProjectResource googleProjectResource = new GoogleProjectResource();

    public StudyDataProject() {}

    public StudyDataProject(StudyDataProjectSummary studyDataProjectSummary) {
        this.studyDataProjectSummary = studyDataProjectSummary;
    }

    public StudyDataProjectSummary getStudyDataProjectSummary() {
        return studyDataProjectSummary;
    }

    public StudyDataProject setStudyDataProjectSummary(StudyDataProjectSummary studyDataProjectSummary) {
        this.studyDataProjectSummary = studyDataProjectSummary;
        return this;
    }

    public GoogleProjectResource getGoogleProjectResource() {
        return googleProjectResource;
    }

    public StudyDataProject googleProjectResource(GoogleProjectResource projectResource) {
        this.googleProjectResource = projectResource;
        return this;
    }

    public String getGoogleProjectId() {
        return googleProjectResource.getGoogleProjectId();
    }

    public StudyDataProject googleProjectId(String googleProjectId) {
        googleProjectResource.googleProjectId(googleProjectId);
        return this;
    }

    public String getGoogleProjectNumber() {
        return googleProjectResource.getGoogleProjectNumber();
    }

    public StudyDataProject googleProjectNumber(String googleProjectNumber) {
        googleProjectResource.googleProjectNumber(googleProjectNumber);
        return this;
    }

    public UUID getProfileId() {
        return googleProjectResource.getProfileId();
    }

    public StudyDataProject profileId(UUID profileId) {
        googleProjectResource.profileId(profileId);
        return this;
    }

    public UUID getProjectResourceId() {
        return googleProjectResource.getRepositoryId();
    }

    public StudyDataProject projectResourceId(UUID projectResourceId) {
        googleProjectResource.repositoryId(projectResourceId);
        return this;
    }

    public List<String> getServiceIds() {
        return googleProjectResource.getServiceIds();
    }

    public StudyDataProject serviceIds(List<String> serviceIds) {
        googleProjectResource.serviceIds(serviceIds);
        return this;
    }

    public UUID getId() {
        return studyDataProjectSummary.getId();
    }

    public StudyDataProject id(UUID id) {
        studyDataProjectSummary.id(id);
        return this;
    }

    public UUID getStudyId() {
        return studyDataProjectSummary.getStudyId();
    }

    public StudyDataProject studyId(UUID studyId) {
        studyDataProjectSummary.studyId(studyId);
        return this;
    }
}
