package bio.terra.common;

import bio.terra.model.TableDataType;
import java.util.Objects;
import java.util.Set;
import javax.ws.rs.NotSupportedException;

public class SynapseColumn extends Column {
  private static final Set<TableDataType> FILE_TYPES =
      Set.of(TableDataType.FILEREF, TableDataType.DIRREF);
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

  public boolean getIsFileType() {
    return FILE_TYPES.contains(getType());
  }

  public static String translateDataType(TableDataType datatype, boolean isArrayOf) {
    if (isArrayOf) {
      return "varchar(8000)";
    }
    return switch (datatype) {
      case BOOLEAN -> "bit";
      case BYTES -> "varbinary";
      case DATE -> "date";
      case DATETIME, TIMESTAMP -> "datetime2";
      case DIRREF, FILEREF -> "varchar(36)";
      case FLOAT, FLOAT64 -> "float";
      case INTEGER -> "numeric(10, 0)";
      case INT64 -> "numeric(19, 0)";
      case NUMERIC -> "real";
      case TEXT, STRING -> "varchar(8000)";
      case TIME -> "time";
        // Data of type RECORD contains table-like that can be nested or repeated
        // It's provided in JSON format, making it hard to parse from inside a CSV/JSON ingest
      case RECORD -> throw new NotSupportedException(
          "RECORD type is not yet supported for synapse");
    };
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    SynapseColumn that = (SynapseColumn) o;
    return requiresCollate == that.requiresCollate
        && requiresJSONCast == that.requiresJSONCast
        && synapseDataType.equals(that.synapseDataType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), synapseDataType, requiresCollate, requiresJSONCast);
  }
}
