package bio.terra.metadata;

import java.util.UUID;

public class AssetRelationship {
    private UUID id;
    private StudyRelationship studyRelationship;

    public AssetRelationship(StudyRelationship studyRelationship) {
        this.studyRelationship = studyRelationship;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public StudyRelationship getStudyRelationship() {
        return studyRelationship;
    }
}
