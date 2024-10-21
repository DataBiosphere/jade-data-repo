package bio.terra.common;

import bio.terra.model.SqlSortDirectionAscDefault;
import bio.terra.model.SqlSortDirectionDescDefault;

public enum SqlSortDirection {
  ASC,
  DESC;

  public static SqlSortDirection from(SqlSortDirectionAscDefault direction) {
    // Defaults set for enums in the open-api spec can still get set to null,
    // either directly by the user or by excluding the field from the request.
    if (direction == null) {
      return ASC;
    }
    return direction == SqlSortDirectionAscDefault.ASC ? ASC : DESC;
  }

  public static SqlSortDirection from(SqlSortDirectionDescDefault direction) {
    // Defaults set for enums in the open-api spec can still get set to null,
    // either directly by the user or by excluding the field from the request.
    if (direction == null) {
      return DESC;
    }
    return direction == SqlSortDirectionDescDefault.ASC ? ASC : DESC;
  }
}
