package bio.terra.service.snapshotbuilder.query.tables;

import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.utils.QueryBuilderFactory;
import bio.terra.service.snapshotbuilder.utils.constants.Person;
import jakarta.annotation.Nullable;

public class DomainOccurrence extends TableVariable {

  private DomainOccurrence(
      TablePointer tablePointer,
      @Nullable String joinField,
      @Nullable FieldVariable joinFieldOnParent,
      boolean isLeftJoin) {
    super(tablePointer, joinField, joinFieldOnParent, isLeftJoin);
  }

  public FieldVariable getCountPerson() {
    return this.makeFieldVariable(Person.PERSON_ID, "COUNT", QueryBuilderFactory.COUNT, true);
  }

  public static class Builder extends TableVariable.Builder<DomainOccurrence> {
    public DomainOccurrence build() {
      return new DomainOccurrence(
          getDomainOptionTablePointer(), getJoinField(), getJoinFieldOnParent(), isLeftJoin());
    }
  }
}
