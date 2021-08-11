package bio.terra.common;

import bio.terra.model.TableDataType;

public class SynapseColumn extends Column {
  private String synapseDataType;
  private boolean requiresCollate;
  private boolean requiresJSONCast;

  public SynapseColumn() {}

  public SynapseColumn(SynapseColumn fromColumn) {
    this.synapseDataType = fromColumn.synapseDataType;
    this.requiresCollate = fromColumn.requiresCollate;
    this.requiresJSONCast = fromColumn.requiresJSONCast;
  }

  public String getSynapseDataType() {
    return synapseDataType;
  }

  public SynapseColumn synapseDataType(String synapseDataType) {
    this.synapseDataType = synapseDataType;
    return this;
  }

  public boolean getRequiresCollate() {
    return requiresCollate;
  }

  public SynapseColumn requiresCollate(boolean requiresCollate) {
    this.requiresCollate = requiresCollate;
    return this;
  }

  public boolean getRequiresJSONCast() {
    return requiresJSONCast;
  }

  public SynapseColumn requiresJSONCast(boolean requiresJSONCast) {
    this.requiresJSONCast = requiresJSONCast;
    return this;
  }

  static String translateDataType(TableDataType datatype, boolean isArrayOf) {
    if (isArrayOf) {
      return "varchar(8000)";
    }
    switch (datatype) {
      case BOOLEAN:
        return "bit";
      case BYTES:
        return "varbinary";
      case DATE:
        return "date";
      case DATETIME:
      case TIMESTAMP:
        return "datetime2";
      case DIRREF:
      case FILEREF:
        return "varchar(36)";
      case FLOAT:
      case FLOAT64:
        return "real";
      case INTEGER:
        return "int";
      case INT64:
        return "bigint";
      case NUMERIC:
        return "decimal";
      case TEXT:
      case STRING:
        return "varchar(8000)";
      case TIME:
        return "time";
      default:
        throw new IllegalArgumentException("Unknown datatype '" + datatype + "'");
    }
  }

  static boolean checkForCollateArgRequirement(TableDataType dataType, boolean isArrayOf) {
    if (isArrayOf) {
      return true;
    }
    switch (dataType) {
      case DIRREF:
      case FILEREF:
      case TEXT:
      case STRING:
        return true;
      default:
        return false;
    }
  }

  static boolean checkForJSONCastRequirement(TableDataType dataType, boolean isArrayOf) {
    if (isArrayOf) {
      return false;
    }
    switch (dataType) {
      case DIRREF:
      case FILEREF:
      case TEXT:
      case STRING:
        return false;
      default:
        return true;
    }
  }
}
