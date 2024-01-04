package bio.terra.service.tabulardata;

import bio.terra.common.Column;
import bio.terra.common.Relationship;
import bio.terra.common.Table;
import bio.terra.service.dataset.AssetRelationship;
import bio.terra.service.dataset.AssetSpecification;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WalkRelationship {
  private static final Logger logger = LoggerFactory.getLogger(WalkRelationship.class);

  public enum WalkDirection {
    FROM_TO,
    TO_FROM
  }

  private String[] columnNames;
  private boolean[] columnIsArrayOf;
  private String[] tableNames;
  private UUID[] tableIds;
  private boolean visited; // marks that we have been here before
  private int fromIndex; // index of table we are walking from
  private int toIndex; // index of table we are walking to

  public static List<WalkRelationship> ofAssetSpecification(AssetSpecification assetSpecification) {
    List<WalkRelationship> walklist = new ArrayList<>();
    for (AssetRelationship assetRelationship : assetSpecification.getAssetRelationships()) {
      walklist.add(WalkRelationship.ofAssetRelationship(assetRelationship));
    }
    return walklist;
  }

  public static WalkRelationship ofAssetRelationship(AssetRelationship assetRelationship) {
    Relationship datasetRelationship = assetRelationship.getDatasetRelationship();
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
    tableIds = new UUID[2];
    visited = false;
    fromIndex = 0;
    toIndex = 1;
  }

  public WalkRelationship fromTable(Table table) {
    this.tableNames[0] = table.getName();
    this.tableIds[0] = table.getId();
    return this;
  }

  public WalkRelationship fromColumn(Column column) {
    this.columnNames[0] = column.getName();
    this.columnIsArrayOf[0] = column.isArrayOf();
    return this;
  }

  public WalkRelationship toTable(Table table) {
    this.tableNames[1] = table.getName();
    this.tableIds[1] = table.getId();
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

  public UUID getFromTableId() {
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

  public UUID getToTableId() {
    return tableIds[toIndex];
  }

  public String getToColumnName() {
    return columnNames[toIndex];
  }

  public Boolean getToColumnIsArray() {
    return columnIsArrayOf[toIndex];
  }

  /**
   * Determines whether we create a snapshot table based on this relationship Sets the Walk
   * Direction - This sets the "TO" and "FROM" tables of the WalkRelationship so that the "FROM"
   * table is the table associated with the startTableId
   *
   * @param startTableId UUID for table designated as the "FROM" table of the relationship
   * @return Returns boolean indicating whether the relationship should be visited Returns true if
   *     we should visit the relationship and marks the relationship as visited Returns false if the
   *     relationship has already been visited Returns false if the "TO" and "FROM" tables are not
   *     connected via the relationship
   */
  public boolean visitRelationship(UUID startTableId) {
    if (this.isVisited()) {
      return false;
    }

    if (startTableId.equals(this.getFromTableId())) {
      this.setDirection(WalkRelationship.WalkDirection.FROM_TO);
    } else if (startTableId.equals(this.getToTableId())) {
      this.setDirection(WalkRelationship.WalkDirection.TO_FROM);
    } else {
      // This relationship is not connected to the start table
      return false;
    }
    logger.info(
        "The relationship is being set from column {} in table {} to column {} in table {}",
        this.getFromColumnName(),
        this.getFromTableName(),
        this.getToColumnName(),
        this.getToTableName());

    this.setVisited();
    return true;
  }
}
