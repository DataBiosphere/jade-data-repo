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
        return "float";
      case FLOAT64:
        return "float";
      case INTEGER:
        return "int";
      case INT64:
        return "bigint";
      case NUMERIC:
        return "real";
      case TEXT:
      case STRING:
        return "varchar(8000)";
      case TIME:
        return "time";
        // Data of type RECORD contains table-like that can be nested or repeated
        // It's provided in JSON format, making it hard to parse from inside a CSV/JSON ingest
      case RECORD:
        throw new NotSupportedException("RECORD type is not yet supported for synapse");
      case JSON:
        throw new NotSupportedException("JSON type is not yet supported for synapse");
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
