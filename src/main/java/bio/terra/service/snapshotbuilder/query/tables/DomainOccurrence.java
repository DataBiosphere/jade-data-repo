package bio.terra.service.snapshotbuilder.query.tables;

import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.utils.constants.ConditionOccurrence;

public class DomainOccurrence extends Table {

  private SnapshotBuilderDomainOption domainOption;

  private DomainOccurrence(TablePointer tablePointer) {
    super(TableVariable.forPrimary(tablePointer));
  }

  public DomainOccurrence(SnapshotBuilderDomainOption domainOption) {
    super(TableVariable.forPrimary(TablePointer.fromTableName(domainOption.getTableName())));
    this.domainOption = domainOption;
  }

  private DomainOccurrence(
      TablePointer tablePointer, String joinField, FieldVariable joinFieldOnParent) {
    super(TableVariable.forJoined(tablePointer, joinField, joinFieldOnParent));
  }

  public static DomainOccurrence asPrimary(TablePointer tablePointer) {
    return new DomainOccurrence(tablePointer);
  }

  public static DomainOccurrence leftJoinOnDescendantConcept(SnapshotBuilderDomainOption domainOption) {
    return new DomainOccurrence(
        TablePointer.fromTableName(domainOption.getTableName()),
        domainOption.getColumnName(),
        ConceptAncestor.asPrimary().descendant_concept_id());
  }

  public static DomainOccurrence joinOnPersonId(String tableName, FieldVariable joinFieldOnParent) {
    return new DomainOccurrence(
        TablePointer.fromTableName(tableName), Person.PERSON_ID, joinFieldOnParent);
  }

  public FieldVariable getJoinColumn() {
    return getFieldVariable(this.domainOption.getColumnName());
  }

  public FieldVariable getConditionConceptId() {
    return getFieldVariable(ConditionOccurrence.CONDITION_CONCEPT_ID);
  }

  public FieldVariable getPerson() {
    return getFieldVariable(Person.PERSON_ID);
  }

  public FieldVariable getCountPerson() {
    return getFieldVariable(Person.PERSON_ID);
  }
}
