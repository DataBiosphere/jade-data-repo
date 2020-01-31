package bio.terra.service.dataset;

import bio.terra.common.Column;
import bio.terra.common.Table;

import java.util.UUID;

public class DatasetRelationship {

    private UUID id;
    private String name;
    private Column fromColumn;
    private Table fromTable;
    private Column toColumn;
    private Table toTable;

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
}
