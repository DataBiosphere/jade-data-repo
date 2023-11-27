package bio.terra.common;

import bio.terra.model.SqlSortDirectionAscDefault;
import bio.terra.model.SqlSortDirectionDescDefault;

public enum SqlSortDirection {
  ASC,
  DESC;

  public static SqlSortDirection from(SqlSortDirectionAscDefault direction) {
    return direction == SqlSortDirectionAscDefault.ASC ? ASC : DESC;
  }

  public static SqlSortDirection from(SqlSortDirectionDescDefault direction) {
    return direction == SqlSortDirectionDescDefault.ASC ? ASC : DESC;
  }
}
