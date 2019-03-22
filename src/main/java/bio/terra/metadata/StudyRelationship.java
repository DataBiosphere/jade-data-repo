package bio.terra.metadata;

import bio.terra.model.RelationshipTermModel.CardinalityEnum;

import java.util.UUID;

public class StudyRelationship {

    private UUID id;
    private String name;
    private Column fromColumn;
    private Table fromTable;
    private CardinalityEnum fromCardinality;
    private Column toColumn;
    private Table toTable;
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

    public Table getFromTable() {
        return fromTable;
    }

    public StudyRelationship fromTable(Table fromTable) {
        this.fromTable = fromTable;
        return this;
    }

    public Column getFromColumn() {
        return fromColumn;
    }

    public StudyRelationship fromColumn(Column from) {
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

    public Table getToTable() {
        return toTable;
    }

    public StudyRelationship toTable(Table toTable) {
        this.toTable = toTable;
        return this;
    }

    public Column getToColumn() {
        return toColumn;
    }

    public StudyRelationship toColumn(Column to) {
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
