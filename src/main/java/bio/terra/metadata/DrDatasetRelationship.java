package bio.terra.metadata;

import bio.terra.model.RelationshipTermModel.CardinalityEnum;

import java.util.UUID;

public class DrDatasetRelationship {

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

    public DrDatasetRelationship id(UUID id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public DrDatasetRelationship name(String name) {
        this.name = name;
        return this;
    }

    public Table getFromTable() {
        return fromTable;
    }

    public DrDatasetRelationship fromTable(Table fromTable) {
        this.fromTable = fromTable;
        return this;
    }

    public Column getFromColumn() {
        return fromColumn;
    }

    public DrDatasetRelationship fromColumn(Column from) {
        this.fromColumn = from;
        return this;
    }

    public CardinalityEnum getFromCardinality() {
        return fromCardinality;
    }

    public DrDatasetRelationship fromCardinality(CardinalityEnum fromCardinality) {
        this.fromCardinality = fromCardinality;
        return this;
    }

    public Table getToTable() {
        return toTable;
    }

    public DrDatasetRelationship toTable(Table toTable) {
        this.toTable = toTable;
        return this;
    }

    public Column getToColumn() {
        return toColumn;
    }

    public DrDatasetRelationship toColumn(Column to) {
        this.toColumn = to;
        return this;
    }

    public CardinalityEnum getToCardinality() {
        return toCardinality;
    }

    public DrDatasetRelationship toCardinality(CardinalityEnum toCardinality) {
        this.toCardinality = toCardinality;
        return this;
    }
}
