package bio.terra.service.snapshotbuilder.query.tables;

import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.utils.QueryBuilderFactory;
import bio.terra.service.snapshotbuilder.utils.constants.Person;
import jakarta.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class DomainOccurrence extends TableVariable {

  private final Map<String, FieldVariable> fields = new HashMap<>();

  private DomainOccurrence(
      TablePointer tablePointer,
      @Nullable String joinField,
      @Nullable FieldVariable joinFieldOnParent,
      boolean isLeftJoin) {
    super(tablePointer, joinField, joinFieldOnParent, isLeftJoin);
  }

  public FieldVariable getFieldVariable(String fieldName) {
    return fields.computeIfAbsent(
        fieldName,
        _key -> this.makeFieldVariable(fieldName, "COUNT", QueryBuilderFactory.COUNT, true));
  }

  public FieldVariable getCountPerson() {
    return getFieldVariable(Person.PERSON_ID);
  }

  public static class Builder extends TableVariable.Builder<DomainOccurrence> {
    public DomainOccurrence build() {
      return new DomainOccurrence(
          getDomainOptionTablePointer(), getJoinField(), getJoinFieldOnParent(), isLeftJoin());
    }
  }
}
