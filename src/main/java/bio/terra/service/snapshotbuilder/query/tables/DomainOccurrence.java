package bio.terra.service.snapshotbuilder.query.tables;

import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.utils.QueryBuilderFactory;
import bio.terra.service.snapshotbuilder.utils.constants.Person;

public class DomainOccurrence extends TableVariable {

  public DomainOccurrence(TableVariable.Builder tableVariableBuilder) {
    super(
        tableVariableBuilder.getDomainOptionTablePointer(),
        tableVariableBuilder.getJoinField(),
        tableVariableBuilder.getJoinFieldOnParent(),
        tableVariableBuilder.isLeftJoin());
  }

  public FieldVariable getCountPerson() {
    return this.makeFieldVariable(Person.PERSON_ID, "COUNT", QueryBuilderFactory.COUNT, true);
  }
}
