package bio.terra.service.tabulardata.google;

import bio.terra.service.dataset.AssetRelationship;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.DatasetRelationship;
import bio.terra.common.Table;
import bio.terra.common.Column;

import java.util.ArrayList;
import java.util.List;

public class WalkRelationship {
    public enum WalkDirection {
        FROM_TO,
        TO_FROM
    }

    private String[] columnNames;
    private boolean[] columnIsArrayOf;
    private String[] tableNames;
    private String[] tableIds;
    private boolean visited; // marks that we have been here before
    private int fromIndex;   // index of table we are walking from
    private int toIndex;     // index of table we are walking to


    public static List<WalkRelationship> ofAssetSpecification(AssetSpecification assetSpecification) {
        List<WalkRelationship> walklist = new ArrayList<>();
        for (AssetRelationship assetRelationship : assetSpecification.getAssetRelationships()) {
            walklist.add(WalkRelationship.ofAssetRelationship(assetRelationship));
        }
        return walklist;
    }

    public static WalkRelationship ofAssetRelationship(AssetRelationship assetRelationship) {
        DatasetRelationship datasetRelationship = assetRelationship.getDatasetRelationship();
        return new WalkRelationship()
                .fromColumn(datasetRelationship.getFromColumn())
                .fromTable(datasetRelationship.getFromTable())
                .toColumn(datasetRelationship.getToColumn())
                .toTable(datasetRelationship.getToTable());
    }

    public WalkRelationship() {
        columnNames = new String[2];
        columnIsArrayOf = new boolean[2];
        tableNames = new String[2];
        tableIds = new String[2];
        visited = false;
        fromIndex = 0;
        toIndex = 1;

    }

    public WalkRelationship fromTable(Table table) {
        this.tableNames[0] = table.getName();
        this.tableIds[0] = table.getId().toString();
        return this;
    }

    public WalkRelationship fromColumn(Column column) {
        this.columnNames[0] = column.getName();
        this.columnIsArrayOf[0] = column.isArrayOf();
        return this;
    }

    public WalkRelationship toTable(Table table) {
        this.tableNames[1] = table.getName();
        this.tableIds[1] = table.getId().toString();
        return this;
    }

    public WalkRelationship toColumn(Column column) {
        this.columnNames[1] = column.getName();
        this.columnIsArrayOf[1] = column.isArrayOf();
        return this;
    }

    public boolean isVisited() {
        return visited;
    }

    public void setVisited() {
        visited = true;
    }

    public void setDirection(WalkDirection direction) {
        if (direction == WalkDirection.FROM_TO) {
            fromIndex = 0;
            toIndex = 1;
        } else {
            fromIndex = 1;
            toIndex = 0;
        }
    }

    public String getFromTableName() {
        return tableNames[fromIndex];
    }

    public String getFromTableId() {
        return tableIds[fromIndex];
    }

    public String getFromColumnName() {
        return columnNames[fromIndex];
    }

    public Boolean getFromColumnIsArray() {
        return columnIsArrayOf[fromIndex];
    }

    public String getToTableName() {
        return tableNames[toIndex];
    }

    public String getToTableId() {
        return tableIds[toIndex];
    }

    public String getToColumnName() {
        return columnNames[toIndex];
    }

    public Boolean getToColumnIsArray() {
        return columnIsArrayOf[toIndex];
    }
}
