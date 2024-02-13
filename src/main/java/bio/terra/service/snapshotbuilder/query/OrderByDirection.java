package bio.terra.service.snapshotbuilder.query;

import bio.terra.common.CloudPlatformWrapper;

public enum OrderByDirection implements SqlExpression {
  ASCENDING("ASC"),
  DESCENDING("DESC");

  private final String sql;

  OrderByDirection(String sql) {
    this.sql = sql;
  }

  @Override
  public String renderSQL(CloudPlatformWrapper platform) {
    return sql;
  }
}
