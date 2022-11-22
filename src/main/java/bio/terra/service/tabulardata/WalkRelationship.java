package bio.terra.service.tabulardata;

import bio.terra.common.Column;
import bio.terra.common.Relationship;
import bio.terra.common.Table;
import bio.terra.service.dataset.AssetRelationship;
import bio.terra.service.dataset.AssetSpecification;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
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
  private String[] tableIds;
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

  // TODO -figure out better name for this method
  // This returns whether or not we should actually walk this relationship
  public boolean processRelationship(String startTableId) {
    if (this.isVisited()) {
      return false;
    }

    // NOTE: setting the direction tells the WalkRelationship to change its meaning of from and
    // to.
    // When constructed, it is always in the FROM_TO direction.
    if (StringUtils.equals(startTableId, this.getFromTableId())) {
      this.setDirection(WalkRelationship.WalkDirection.FROM_TO);
    } else if (StringUtils.equals(startTableId, this.getToTableId())) {
      this.setDirection(WalkRelationship.WalkDirection.TO_FROM);
    } else {
      // This relationship is not connected to the start table
      return false;
    }
    logger.info(
        "[assetTest] The relationship is being set from column {} in table {} to column {} in table {}",
        this.getFromColumnName(),
        this.getFromTableName(),
        this.getToColumnName(),
        this.getToTableName());

    this.setVisited();
    return true;
  }
}
