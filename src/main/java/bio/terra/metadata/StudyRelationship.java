package bio.terra.metadata;

import bio.terra.model.RelationshipTermModel.CardinalityEnum;

import java.util.UUID;

public class StudyRelationship {

    private UUID id;
    private String name;
    private StudyTableColumn fromColumn;
    private StudyTable fromTable;
    private CardinalityEnum fromCardinality;
    private StudyTableColumn toColumn;
    private StudyTable toTable;
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

    public StudyTable getFromTable() {
        return fromTable;
    }

    public StudyRelationship fromTable(StudyTable fromTable) {
        this.fromTable = fromTable;
        return this;
    }

    public StudyTableColumn getFromColumn() {
        return fromColumn;
    }

    public StudyRelationship fromColumn(StudyTableColumn from) {
        this.fromColumn = from;
        return this;
    }

    public CardinalityEnum getFromCardinality() {
        return fromCardinality;
    }

    public StudyRelationship fromCardinality(CardinalityEnum fromCardinality) {
        this.fromCardinality = fromCardinality;
        return this;
    }

    public StudyTable getToTable() {
        return toTable;
    }

    public StudyRelationship toTable(StudyTable toTable) {
        this.toTable = toTable;
        return this;
    }

    public StudyTableColumn getToColumn() {
        return toColumn;
    }

    public StudyRelationship toColumn(StudyTableColumn to) {
        this.toColumn = to;
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
