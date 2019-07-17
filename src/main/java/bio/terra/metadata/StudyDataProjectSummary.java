package bio.terra.metadata;

import java.util.UUID;

public class StudyDataProjectSummary {

    private UUID id;
    private UUID studyId;
    private UUID projectResourceId;

    public UUID getId() {
        return id;
    }

    public StudyDataProjectSummary id(UUID id) {
        this.id = id;
        return this;
    }

    public UUID getStudyId() {
        return studyId;
    }

    public StudyDataProjectSummary studyId(UUID studyId) {
        this.studyId = studyId;
        return this;
    }

    public UUID getProjectResourceId() {
        return projectResourceId;
    }

    public StudyDataProjectSummary projectResourceId(UUID projectResourceId) {
        this.projectResourceId = projectResourceId;
        return this;
    }
}
