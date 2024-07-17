package bio.terra.service.snapshotbuilder.query.table;

import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.SourceVariable;
import bio.terra.service.snapshotbuilder.query.SqlExpression;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import java.util.HashMap;
import java.util.Map;

public class Table implements SqlExpression {
  private final Map<String, FieldVariable> fields = new HashMap<>();
  private final SourceVariable sourceVariable;

  public Table(SourceVariable tableVariable) {
    this.sourceVariable = tableVariable;
  }

  private Table(String tableName) {
    this.sourceVariable = SourceVariable.forPrimary(TablePointer.fromTableName(tableName));
  }

  private Table(
      String tableName, String joinField, FieldVariable joinFieldOnParent, boolean isLeftJoined) {
    this.sourceVariable =
        SourceVariable.forJoined(
            TablePointer.fromTableName(tableName), joinField, joinFieldOnParent, isLeftJoined);
  }

  public static Table asPrimary(String tableName) {
    return new Table(tableName);
  }

  public static Table asJoined(
      String tableName, String joinField, FieldVariable joinFieldOnParent, boolean isLeftJoined) {
    return new Table(tableName, joinField, joinFieldOnParent, isLeftJoined);
  }

  public FieldVariable getFieldVariable(String fieldName) {
    return fields.computeIfAbsent(fieldName, sourceVariable::makeFieldVariable);
  }

  public FieldVariable getFieldVariable(
      String fieldName, String sqlFunctionWrapper, String alias, boolean isDistinct) {
    return fields.computeIfAbsent(
        fieldName,
        (key) -> sourceVariable.makeFieldVariable(key, sqlFunctionWrapper, alias, isDistinct));
  }

  public boolean isPrimary() {
    return this.sourceVariable.isPrimary();
  }

  public String tableName() {
    return this.sourceVariable.getSourcePointer().getSourceName();
  }

  @Override
  public String renderSQL(SqlRenderContext context) {
    return this.sourceVariable.renderSQL(context);
  }
}
