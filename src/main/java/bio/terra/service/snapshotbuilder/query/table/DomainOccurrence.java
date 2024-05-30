package bio.terra.service.snapshotbuilder.query.table;

import static bio.terra.service.snapshotbuilder.query.table.Person.PERSON_ID;

import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.SourceVariable;
import bio.terra.service.snapshotbuilder.query.TablePointer;

public class DomainOccurrence extends Table {

  private final FieldVariable countPerson;
  private final SnapshotBuilderDomainOption domainOption;

  private DomainOccurrence(SnapshotBuilderDomainOption domainOption, SourceVariable tableVariable) {
    super(tableVariable);
    this.domainOption = domainOption;
    this.countPerson = tableVariable.makeFieldVariable(PERSON_ID, "COUNT", "count", true);
  }

  public static DomainOccurrence leftJoinOn(
      SnapshotBuilderDomainOption domainOption, FieldVariable fieldVariable) {
    return new DomainOccurrence(
        domainOption,
        SourceVariable.forJoined(
            TablePointer.fromTableName(domainOption.getTableName()),
            domainOption.getColumnName(),
            fieldVariable,
            true));
  }

  public static DomainOccurrence forPrimary(SnapshotBuilderDomainOption domainOption) {
    return new DomainOccurrence(
        domainOption,
        SourceVariable.forPrimary(TablePointer.fromTableName(domainOption.getTableName())));
  }

  public FieldVariable getJoinColumn() {
    return getFieldVariable(this.domainOption.getColumnName());
  }

  public FieldVariable personId() {
    return getFieldVariable(PERSON_ID);
  }

  public FieldVariable countPerson() {
    return this.countPerson;
  }
}
