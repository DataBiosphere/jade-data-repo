package bio.terra.service.snapshotbuilder;

import bio.terra.common.exception.BadRequestException;
import bio.terra.model.SnapshotBuilderCriteria;
import bio.terra.model.SnapshotBuilderDomainCriteria;
import bio.terra.model.SnapshotBuilderProgramDataListCriteria;
import bio.terra.model.SnapshotBuilderProgramDataRangeCriteria;
import bio.terra.service.snapshotbuilder.query.FieldPointer;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.FunctionFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.SubQueryFilterVariable;
import java.util.List;

public class CriteriaQueryUtils {
  private static BooleanAndOrFilterVariable generateFilterForRangeCriteria(
      SnapshotBuilderProgramDataRangeCriteria rangeCriteria) {
    TablePointer tablePointer = TablePointer.fromTableName("person");
    TableVariable tableVariable = TableVariable.forPrimary(tablePointer);
    return new BooleanAndOrFilterVariable(
        BooleanAndOrFilterVariable.LogicalOperator.AND,
        List.of(
            new BinaryFilterVariable(
                new FieldVariable(
                    new FieldPointer(tablePointer, rangeCriteria.getName()), tableVariable),
                BinaryFilterVariable.BinaryOperator.GREATER_THAN_OR_EQUAL,
                new Literal(rangeCriteria.getLow().doubleValue())),
            new BinaryFilterVariable(
                new FieldVariable(
                    new FieldPointer(tablePointer, rangeCriteria.getName()), tableVariable),
                BinaryFilterVariable.BinaryOperator.LESS_THAN_OR_EQUAL,
                new Literal(rangeCriteria.getHigh().doubleValue()))));
  }

  private static FunctionFilterVariable generateFilterForListCriteria(
      SnapshotBuilderProgramDataListCriteria listCriteria) {
    TablePointer tablePointer = TablePointer.fromTableName("person");
    TableVariable tableVariable = TableVariable.forPrimary(tablePointer);
    TablePointer conceptPointer = TablePointer.fromTableName("concept");
    TableVariable conceptPointerVariable =
        TableVariable.forJoined(
            conceptPointer,
            "concept_id",
            new FieldVariable(
                new FieldPointer(tablePointer, listCriteria.getName()), tableVariable));
    return new FunctionFilterVariable(
        FunctionFilterVariable.FunctionTemplate.IN,
        new FieldVariable(new FieldPointer(conceptPointer, "concept_id"), conceptPointerVariable),
        listCriteria.getValues().stream()
            .map(value -> new Literal(value.doubleValue()))
            .toArray(Literal[]::new));
  }

  private record OccurrenceTable(String tableName, String idColumnName) {}

  private static OccurrenceTable getOccurrenceTableFromDomainCriteria(
      SnapshotBuilderDomainCriteria domainCriteria) {
    // TODO: Load this information from snapshot builder settings, rather than hardcode
    return switch (domainCriteria.getDomainName()) {
      case "Condition" -> new OccurrenceTable("condition_occurrence", "condition_concept_id");
      case "Measurement" -> new OccurrenceTable("measurement", "measurement_concept_id");
      case "Visit" -> new OccurrenceTable("visit_occurrence", "visit_concept_id");
      case "Procedure" -> new OccurrenceTable("procedure_occurrence", "procedure_concept_id");
      case "Observation" -> new OccurrenceTable("observation", "observation_concept_id");
      case "Device" -> new OccurrenceTable("device_exposure", "device_concept_id");
      case "Drug" -> new OccurrenceTable("drug_exposure", "drug_concept_id");
      default -> throw new BadRequestException(
          String.format("Domain %s is not found in dataset", domainCriteria.getDomainName()));
    };
  }

  private static SubQueryFilterVariable generateFilterForDomainCriteria(
      SnapshotBuilderDomainCriteria domainCriteria) {
    TablePointer tablePointer = TablePointer.fromTableName("person");
    TableVariable tableVariable = TableVariable.forPrimary(tablePointer);

    OccurrenceTable occurrenceTable = getOccurrenceTableFromDomainCriteria(domainCriteria);

    TablePointer occurrencePointer = TablePointer.fromTableName(occurrenceTable.tableName());
    TableVariable occurrenceVariable = TableVariable.forPrimary(occurrencePointer);

    TablePointer ancestorPointer = TablePointer.fromTableName("concept_ancestor");
    TableVariable ancestorVariable =
        TableVariable.forJoined(
            ancestorPointer,
            "ancestor_concept_id",
            new FieldVariable(
                new FieldPointer(occurrencePointer, occurrenceTable.idColumnName()),
                occurrenceVariable));

    return new SubQueryFilterVariable(
        new FieldVariable(new FieldPointer(tablePointer, "person_id"), tableVariable),
        SubQueryFilterVariable.Operator.IN,
        new Query(
            List.of(
                new FieldVariable(
                    new FieldPointer(occurrencePointer, "person_id"), occurrenceVariable)),
            List.of(occurrenceVariable, ancestorVariable),
            new BooleanAndOrFilterVariable(
                BooleanAndOrFilterVariable.LogicalOperator.OR,
                List.of(
                    new BinaryFilterVariable(
                        new FieldVariable(
                            new FieldPointer(occurrencePointer, occurrenceTable.idColumnName()),
                            occurrenceVariable),
                        BinaryFilterVariable.BinaryOperator.EQUALS,
                        new Literal(domainCriteria.getId().doubleValue())),
                    new BinaryFilterVariable(
                        new FieldVariable(
                            new FieldPointer(ancestorPointer, "ancestor_concept_id"),
                            ancestorVariable),
                        BinaryFilterVariable.BinaryOperator.EQUALS,
                        new Literal(domainCriteria.getId().doubleValue()))))));
  }

  public static FilterVariable generateFilterForCriteria(SnapshotBuilderCriteria criteria) {
    switch (criteria.getKind()) {
      case LIST -> {
        SnapshotBuilderProgramDataListCriteria listCriteria =
            (SnapshotBuilderProgramDataListCriteria) criteria;
        return generateFilterForListCriteria(listCriteria);
      }
      case RANGE -> {
        SnapshotBuilderProgramDataRangeCriteria rangeCriteria =
            (SnapshotBuilderProgramDataRangeCriteria) criteria;
        return generateFilterForRangeCriteria(rangeCriteria);
      }
      case DOMAIN -> {
        SnapshotBuilderDomainCriteria domainCriteria = (SnapshotBuilderDomainCriteria) criteria;
        return generateFilterForDomainCriteria(domainCriteria);
      }
      default -> throw new BadRequestException(String.format(
          "Criteria kind %s not one of: domain, range, list", criteria.getKind()));
    }
  }
}
