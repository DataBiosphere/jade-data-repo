package bio.terra.service.dataset;

import bio.terra.common.Column;
import bio.terra.common.Table;
import bio.terra.model.RelationshipTermModel.CardinalityEnum;

import java.util.UUID;

public class DatasetRelationship {

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

    public DatasetRelationship id(UUID id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public DatasetRelationship name(String name) {
        this.name = name;
        return this;
    }

    public Table getFromTable() {
        return fromTable;
    }

    public DatasetRelationship fromTable(Table fromTable) {
        this.fromTable = fromTable;
        return this;
    }

    public Column getFromColumn() {
        return fromColumn;
    }

    public DatasetRelationship fromColumn(Column from) {
        this.fromColumn = from;
        return this;
    }

    public CardinalityEnum getFromCardinality() {
        return fromCardinality;
    }

    public DatasetRelationship fromCardinality(CardinalityEnum fromCardinality) {
        this.fromCardinality = fromCardinality;
        return this;
    }

    public Table getToTable() {
        return toTable;
    }

    public DatasetRelationship toTable(Table toTable) {
        this.toTable = toTable;
        return this;
    }

    public Column getToColumn() {
        return toColumn;
    }

    public DatasetRelationship toColumn(Column to) {
        this.toColumn = to;
        return this;
    }

    public CardinalityEnum getToCardinality() {
        return toCardinality;
    }

    public DatasetRelationship toCardinality(CardinalityEnum toCardinality) {
        this.toCardinality = toCardinality;
        return this;
    }
}
