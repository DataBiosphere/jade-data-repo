package bio.terra.common;

import bio.terra.model.TableDataType;
import jakarta.ws.rs.NotSupportedException;
import java.util.Objects;
import java.util.Set;

public class SynapseColumn extends Column {
  private static final Set<TableDataType> FILE_TYPES =
      Set.of(TableDataType.FILEREF, TableDataType.DIRREF);
  private String synapseDataType;
  private String synapseDataTypeForCsv;
  private boolean requiresCollate;
  private boolean requiresJSONCast;

  private boolean requiresTypeCast;

  public SynapseColumn() {}

  public SynapseColumn(SynapseColumn fromColumn) {
    this.synapseDataType = fromColumn.synapseDataType;
    this.requiresCollate = fromColumn.requiresCollate;
    this.requiresJSONCast = fromColumn.requiresJSONCast;
    this.requiresTypeCast = fromColumn.requiresTypeCast;
  }

  public String getSynapseDataType() {
    return synapseDataType;
  }

  public SynapseColumn synapseDataType(String synapseDataType) {
    this.synapseDataType = synapseDataType;
    return this;
  }

  public String getSynapseDataTypeForCsv() {
    return synapseDataTypeForCsv;
  }

  public SynapseColumn synapseDataTypeForCsv(String synapseDataTypeForCsv) {
    this.synapseDataTypeForCsv = synapseDataTypeForCsv;
    return this;
  }

  public boolean getRequiresTypeCast() {
    return requiresTypeCast;
  }

  public SynapseColumn requiresTypeCast(boolean requiresTypeCast) {
    this.requiresTypeCast = requiresTypeCast;
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
    return translateDataType(datatype, isArrayOf, false);
  }

  public static String translateDataType(
      TableDataType datatype, boolean isArrayOf, boolean isForCsv) {
    if (isArrayOf) {
      return "varchar(%s)".formatted(isForCsv ? "8000" : "max");
    }
    return switch (datatype) {
      case BOOLEAN -> "bit";
      case BYTES -> "varbinary";
      case DATE -> "date";
      case DATETIME, TIMESTAMP -> "datetime2";
      case FLOAT, FLOAT64 -> "float";
      case INTEGER -> "numeric(10, 0)";
      case INT64 -> "numeric(19, 0)";
      case NUMERIC -> "real";
        // DIRREF and FILEREF store a UUID on ingest
        // But, are translated to DRS URI on Snapshot Creation
        // Note that the Synapse CSV parser does not support varchars larger than 8000 bytes
      case DIRREF, FILEREF, TEXT, STRING -> "varchar(%s)".formatted(isForCsv ? "8000" : "max");
      case TIME -> "time";
        // Data of type RECORD contains table-like that can be nested or repeated
        // It's provided in JSON format, making it hard to parse from inside a CSV/JSON ingest
      case RECORD ->
          throw new NotSupportedException("RECORD type is not yet supported for synapse");
    };
  }

  // Cast needed for backwards compatibility after Synapse type change
  // Sept 2023 int moved to numeric(10,0) and int64 moved to numeric(19,0)
  static boolean checkForCastTypeArgRequirement(TableDataType dataType, boolean isArrayOf) {
    if (isArrayOf) {
      return false;
    }
    return switch (dataType) {
      case INTEGER, INT64 -> true;
      default -> false;
    };
  }

  static boolean checkForCollateArgRequirement(TableDataType dataType, boolean isArrayOf) {
    if (isArrayOf) {
      return true;
    }
    return switch (dataType) {
      case DIRREF, FILEREF, TEXT, STRING -> true;
      default -> false;
    };
  }

  static boolean checkForJSONCastRequirement(TableDataType dataType, boolean isArrayOf) {
    if (isArrayOf) {
      return false;
    } else {
      return switch (dataType) {
        case TEXT, STRING -> true;
        default -> false;
      };
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
