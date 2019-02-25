package bio.terra.metadata;

import java.util.UUID;

public class AssetRelationship {
    private UUID id;
    private StudyRelationship studyRelationship;

    public UUID getId() {
        return id;
    }

    public AssetRelationship id(UUID id) {
        this.id = id;
        return this;
    }

    public StudyRelationship getStudyRelationship() {
        return studyRelationship;
    }

    public AssetRelationship studyRelationship(StudyRelationship studyRelationship) {
        this.studyRelationship = studyRelationship;
        return this;
    }
}
