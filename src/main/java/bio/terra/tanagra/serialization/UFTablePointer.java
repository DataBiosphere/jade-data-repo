package bio.terra.tanagra.serialization;

import bio.terra.tanagra.query.TablePointer;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

/**
 * External representation of a pointer to a table in the underlying data.
 *
 * <p>This is a POJO class intended for serialization. This JSON format is user-facing.
 */
@JsonDeserialize(builder = UFTablePointer.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UFTablePointer {
  private final String table;
  private final UFFilter filter;
  private final String rawSql;
  private final String rawSqlFile;

  public UFTablePointer(TablePointer tablePointer) {
    this.table = tablePointer.getTableName();
    this.filter = tablePointer.hasTableFilter() ? tablePointer.getTableFilter().serialize() : null;
    this.rawSql = tablePointer.getSql();
    // Separate file for SQL string available for input/deserialization, not
    // output/re-serialization.
    this.rawSqlFile = null;
  }

  protected UFTablePointer(Builder builder) {
    this.table = builder.table;
    this.filter = builder.filter;
    this.rawSql = builder.rawSql;
    this.rawSqlFile = builder.rawSqlFile;
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static class Builder {
    private String table;
    private UFFilter filter;
    private String rawSql;
    private String rawSqlFile;

    public Builder table(String table) {
      this.table = table;
      return this;
    }

    public Builder filter(UFFilter filter) {
      this.filter = filter;
      return this;
    }

    public Builder rawSql(String rawSql) {
      this.rawSql = rawSql;
      return this;
    }

    public Builder rawSqlFile(String rawSqlFile) {
      this.rawSqlFile = rawSqlFile;
      return this;
    }

    /** Call the private constructor. */
    public UFTablePointer build() {
      return new UFTablePointer(this);
    }
  }

  public String getTable() {
    return table;
  }

  public UFFilter getFilter() {
    return filter;
  }

  public String getRawSql() {
    return rawSql;
  }

  public String getRawSqlFile() {
    return rawSqlFile;
  }
}
