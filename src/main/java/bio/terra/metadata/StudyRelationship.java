package bio.terra.metadata;

import bio.terra.model.RelationshipTermModel.CardinalityEnum;

import java.util.UUID;

public class StudyRelationship {

    private UUID id;
    private String name;
    private StudyTableColumn from;
    private CardinalityEnum fromCardinality;
    private StudyTableColumn to;
    private CardinalityEnum toCardinality;

    public UUID getId() {
        return id;
    }

    public StudyRelationship id(UUID id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public StudyRelationship name(String name) {
        this.name = name;
        return this;
    }

    public StudyTableColumn getFrom() {
        return from;
    }

    public StudyRelationship from(StudyTableColumn from) {
        this.from = from;
        return this;
    }

    public CardinalityEnum getFromCardinality() {
        return fromCardinality;
    }

    public StudyRelationship fromCardinality(CardinalityEnum fromCardinality) {
        this.fromCardinality = fromCardinality;
        return this;
    }

    public StudyTableColumn getTo() {
        return to;
    }

    public StudyRelationship to(StudyTableColumn to) {
        this.to = to;
        return this;
    }

    public CardinalityEnum getToCardinality() {
        return toCardinality;
    }

    public StudyRelationship toCardinality(CardinalityEnum toCardinality) {
        this.toCardinality = toCardinality;
        return this;
    }
}
