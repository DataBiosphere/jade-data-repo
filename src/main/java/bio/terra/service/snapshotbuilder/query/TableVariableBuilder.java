package bio.terra.service.snapshotbuilder.query;

public class TableVariableBuilder {
  private String joinField;
  private FieldVariable joinFieldOnParent;
  private boolean isLeftJoin;
  private TablePointer domainOptionTablePointer;

  public TableVariableBuilder leftJoin(String joinField) {
    this.isLeftJoin = true;
    this.joinField = joinField;
    return this;
  }

  public TableVariableBuilder from(String domainOptionTableName) {
    this.domainOptionTablePointer = TablePointer.fromTableName(domainOptionTableName);
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

  public TablePointer getDomainOptionTablePointer() {
    return this.domainOptionTablePointer;
  }

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
