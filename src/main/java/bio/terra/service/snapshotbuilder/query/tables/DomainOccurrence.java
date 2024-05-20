package bio.terra.service.snapshotbuilder.query.tables;

import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.utils.constants.Person;

public class DomainOccurrence extends Table {

  private DomainOccurrence(
      TablePointer tablePointer, String joinField, FieldVariable joinFieldOnParent) {
    super(TableVariable.forJoined(tablePointer, joinField, joinFieldOnParent));
  }

  public static DomainOccurrence leftJoinOnDescendantConcept(String tableName, String joinField) {
    return new DomainOccurrence(
        TablePointer.fromTableName(tableName),
        joinField,
        ConceptAncestor.asPrimary().descendant_concept_id());
  }

  public FieldVariable getCountPerson() {
    return getFieldVariable(Person.PERSON_ID);
  }
}
