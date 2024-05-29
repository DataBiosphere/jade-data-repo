package bio.terra.service.snapshotbuilder.query.tables;

import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.SourceVariable;
import bio.terra.service.snapshotbuilder.query.SqlExpression;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;
import java.util.HashMap;
import java.util.Map;

public class Table implements SqlExpression {
  private final Map<String, FieldVariable> fields = new HashMap<>();
  private final SourceVariable sourceVariable;

  public Table(SourceVariable tableVariable) {
    this.sourceVariable = tableVariable;
  }

  protected FieldVariable getFieldVariable(String fieldName) {
    return fields.computeIfAbsent(fieldName, sourceVariable::makeFieldVariable);
  }

  public boolean isPrimary() {
    return this.sourceVariable.isPrimary();
  }

  @Override
  public String renderSQL(SqlRenderContext context) {
    return this.sourceVariable.renderSQL(context);
  }
}
