package bio.terra.common;

import bio.terra.model.SqlSortDirectionAscDefault;
import bio.terra.model.SqlSortDirectionDescDefault;

public enum SqlSortDirection {
  ASC,
  DESC;

  public static SqlSortDirection from(SqlSortDirectionAscDefault direction) {
    // Since we don't require the sort direction field, it can still be null
    if (direction == null) {
      return ASC;
    }
    return direction == SqlSortDirectionAscDefault.ASC ? ASC : DESC;
  }

  public static SqlSortDirection from(SqlSortDirectionDescDefault direction) {
    if (direction == null) {
      return DESC;
    }
    return direction == SqlSortDirectionDescDefault.ASC ? ASC : DESC;
  }
}
