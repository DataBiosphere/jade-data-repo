package bio.terra.service.snapshotbuilder.query;

public class TableVariableBuilder {
  private String joinField;
  private FieldVariable joinFieldOnParent;
  private boolean isLeftJoin;
  private String domainOptionTableName;

  public TableVariableBuilder leftJoin(String joinField) {
    this.isLeftJoin = true;
    this.joinField = joinField;
    return this;
  }

  public TableVariableBuilder from(String domainOptionTableName) {
    this.domainOptionTableName = domainOptionTableName;
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

  public String getDomainOptionTableName() { return this.domainOptionTableName; }

  public String getJoinField() {
    return this.joinField;
  }

  public FieldVariable getJoinFieldOnParent() {
    return this.joinFieldOnParent;
  }

  public boolean isLeftJoin() {
    return this.isLeftJoin;
  }
}
