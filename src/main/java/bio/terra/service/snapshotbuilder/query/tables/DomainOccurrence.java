package bio.terra.service.snapshotbuilder.query.tables;

import static bio.terra.service.snapshotbuilder.query.tables.Person.PERSON_ID;

import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;

public class DomainOccurrence extends Table {

  private final FieldVariable personCount;
  private final SnapshotBuilderDomainOption domainOption;

  private DomainOccurrence(SnapshotBuilderDomainOption domainOption, TableVariable tableVariable) {
    super(tableVariable);
    this.domainOption = domainOption;
    this.personCount = tableVariable.makeFieldVariable(PERSON_ID, "COUNT", "count", true);
  }

  public static DomainOccurrence leftJoinOn(
      SnapshotBuilderDomainOption domainOption, FieldVariable fieldVariable) {
    return new DomainOccurrence(
        domainOption,
        TableVariable.forJoined(
            TablePointer.fromTableName(domainOption.getTableName()),
            domainOption.getColumnName(),
            fieldVariable,
            true));
  }

  public static DomainOccurrence forPrimary(SnapshotBuilderDomainOption domainOption) {
    return new DomainOccurrence(
        domainOption,
        TableVariable.forPrimary(TablePointer.fromTableName(domainOption.getTableName())));
  }

  public FieldVariable getJoinColumn() {
    return getFieldVariable(this.domainOption.getColumnName());
  }

  public FieldVariable getPerson() {
    return getFieldVariable(PERSON_ID);
  }

  public FieldVariable countPersonId() {
    return personCount;
  }
}
