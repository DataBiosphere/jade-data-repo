package bio.terra.service.snapshotbuilder.query.tables;

import static bio.terra.service.snapshotbuilder.query.tables.Person.PERSON_ID;

import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;

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
      TablePointer tablePointer,
      String joinField,
      FieldVariable joinFieldOnParent,
      boolean leftJoin) {
    super(TableVariable.forJoined(tablePointer, joinField, joinFieldOnParent, leftJoin));
  }

  public static DomainOccurrence asPrimary(TablePointer tablePointer) {
    return new DomainOccurrence(tablePointer);
  }

  public static DomainOccurrence leftJoinOnDescendantConcept(
      SnapshotBuilderDomainOption domainOption, ConceptAncestor conceptAncestor) {
    return new DomainOccurrence(
        TablePointer.fromTableName(domainOption.getTableName()),
        domainOption.getColumnName(),
        conceptAncestor.descendant_concept_id(),
        true);
  }

  public FieldVariable getJoinColumn() {
    return getFieldVariable(this.domainOption.getColumnName());
  }

  public FieldVariable getPerson() {
    return getFieldVariable(PERSON_ID);
  }

  public FieldVariable countPersonId() {
    return getFieldVariable(PERSON_ID, "COUNT", "count", true);
  }
}
