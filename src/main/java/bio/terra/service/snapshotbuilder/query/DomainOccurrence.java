package bio.terra.service.snapshotbuilder.query;


public class DomainOccurrence extends TableVariable {

  public DomainOccurrence(TableVariableBuilder tableVariableBuilder) {
    super(tableVariableBuilder.getDomainOptionTableName(), joinField, joinFieldOnParent, isLeftJoin);
  }
}
