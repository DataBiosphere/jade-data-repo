package bio.terra.service.snapshotbuilder.query.tables;

import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.SqlExpression;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import java.util.HashMap;
import java.util.Map;

public class Table implements SqlExpression {
  private final Map<String, FieldVariable> fields = new HashMap<>();
  private final TableVariable tableVariable;

  public Table(TableVariable tableVariable) {
    this.tableVariable = tableVariable;
  }

  protected FieldVariable getFieldVariable(String fieldName) {
    return fields.computeIfAbsent(fieldName, tableVariable::makeFieldVariable);
  }

  public boolean isPrimary() {
    return this.tableVariable.isPrimary();
  }

  @Override
  public String renderSQL(SqlRenderContext context) {
    return this.tableVariable.renderSQL(context);
  }
}
