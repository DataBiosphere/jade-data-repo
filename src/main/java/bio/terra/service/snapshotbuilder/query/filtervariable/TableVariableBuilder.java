package bio.terra.service.snapshotbuilder.query.filtervariable;

import bio.terra.service.snapshotbuilder.query.FieldVariable;

public class TableVariableBuilder {
  private String joinField;
  private FieldVariable joinFieldOnParent;
  private boolean isLeftJoin;

  public TableVariableBuilder leftJoin(String joinField) {
    this.isLeftJoin = true;
    return this;
  }

  public TableVariableBuilder join(String joinField) {
    this.isLeftJoin = false;
    this.joinField = joinField;
    return this;
  }

  public TableVariableBuilder on(FieldVariable joinFieldOnParent) {
    this.joinFieldOnParent = joinFieldOnParent;
    return this;
  }

  public String getJoinField() {
    return joinField;
  }

  public FieldVariable getJoinFieldOnParent() {
    return joinFieldOnParent;
  }

  public boolean isLeftJoin() {
    return isLeftJoin;
  }
}
