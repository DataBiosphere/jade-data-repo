package bio.terra.tanagra.serialization;

import bio.terra.tanagra.query.FieldPointer;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

/**
 * External representation of a pointer to a column in the underlying data.
 *
 * <p>This is a POJO class intended for serialization. This JSON format is user-facing.
 */
@JsonDeserialize(builder = UFFieldPointer.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UFFieldPointer {
  private final String column;
  private final String foreignTable;
  private final String foreignKey;
  private final String foreignColumn;
  private final String
      sqlFunctionWrapper; // can include ${fieldSql} for wrappers that are not just ___(field)

  public UFFieldPointer(FieldPointer fieldPointer) {
    this.column = fieldPointer.getColumnName();
    if (fieldPointer.isForeignKey()) {
      this.foreignTable = fieldPointer.getForeignTablePointer().getTableName();
      this.foreignKey = fieldPointer.getForeignKeyColumnName();
      this.foreignColumn = fieldPointer.getForeignColumnName();
    } else {
      this.foreignTable = null;
      this.foreignKey = null;
      this.foreignColumn = null;
    }
    this.sqlFunctionWrapper = fieldPointer.getSqlFunctionWrapper();
  }

  protected UFFieldPointer(Builder builder) {
    this.column = builder.column;
    this.foreignTable = builder.foreignTable;
    this.foreignKey = builder.foreignKey;
    this.foreignColumn = builder.foreignColumn;
    this.sqlFunctionWrapper = builder.sqlFunctionWrapper;
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static class Builder {
    private String column;
    private String foreignTable;
    private String foreignKey;
    private String foreignColumn;
    private String sqlFunctionWrapper;

    public Builder column(String column) {
      this.column = column;
      return this;
    }

    public Builder foreignTable(String foreignTable) {
      this.foreignTable = foreignTable;
      return this;
    }

    public Builder foreignKey(String foreignKey) {
      this.foreignKey = foreignKey;
      return this;
    }

    public Builder foreignColumn(String foreignColumn) {
      this.foreignColumn = foreignColumn;
      return this;
    }

    public Builder sqlFunctionWrapper(String sqlFunctionWrapper) {
      this.sqlFunctionWrapper = sqlFunctionWrapper;
      return this;
    }

    /** Call the private constructor. */
    public UFFieldPointer build() {
      return new UFFieldPointer(this);
    }
  }

  public String getColumn() {
    return column;
  }

  public String getForeignTable() {
    return foreignTable;
  }

  public String getForeignKey() {
    return foreignKey;
  }

  public String getForeignColumn() {
    return foreignColumn;
  }

  public String getSqlFunctionWrapper() {
    return sqlFunctionWrapper;
  }
}
