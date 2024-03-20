package bio.terra.service.snapshotbuilder;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.SelectExpression;

public class SelectAlias implements SelectExpression {

  private final FieldVariable fieldVariable;
  private final String alias;

  public SelectAlias(FieldVariable fieldVariable, String alias) {
    this.alias = alias;
    this.fieldVariable = fieldVariable;
  }

  @Override
  public String renderSQL(CloudPlatformWrapper platform) {
    return "%s AS %s".formatted(fieldVariable.renderSQL(platform), alias);
  }
}
