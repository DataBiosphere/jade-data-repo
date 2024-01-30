package bio.terra.service.snapshotbuilder.utils;

import static bio.terra.service.snapshotbuilder.utils.QueryBuilderUtils.makeFieldVariable;

import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.TableNameGenerator;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.SubQueryFilterVariable;
import java.util.List;

public class ConceptChildrenQueryBuilder {

  private ConceptChildrenQueryBuilder() {}

  public static String buildConceptChildrenQuery(
      int conceptId, TableNameGenerator tableNameGenerator) {
    TablePointer conceptTablePointer = TablePointer.fromTableName("concept", tableNameGenerator);
    TableVariable conceptTableVariable = TableVariable.forPrimary(conceptTablePointer);
    FieldVariable nameFieldVariable =
        makeFieldVariable(conceptTablePointer, conceptTableVariable, "concept_name");
    FieldVariable idFieldVariable =
        makeFieldVariable(conceptTablePointer, conceptTableVariable, "concept_id");

    TablePointer ancestorTablePointer =
        TablePointer.fromTableName("concept_ancestor", tableNameGenerator);
    TableVariable ancestorTableVariable = TableVariable.forPrimary(ancestorTablePointer);
    FieldVariable descendantFieldVariable =
        makeFieldVariable(ancestorTablePointer, ancestorTableVariable, "descendant_concept_id");

    BinaryFilterVariable whereClause =
        new BinaryFilterVariable(
            makeFieldVariable(ancestorTablePointer, ancestorTableVariable, "ancestor_concept_id"),
            BinaryFilterVariable.BinaryOperator.EQUALS,
            new Literal(conceptId));
    Query subQuery =
        new Query(List.of(descendantFieldVariable), List.of(ancestorTableVariable), whereClause);
    SubQueryFilterVariable subQueryFilterVariable =
        new SubQueryFilterVariable(idFieldVariable, SubQueryFilterVariable.Operator.IN, subQuery);
    Query query =
        new Query(
            List.of(nameFieldVariable, idFieldVariable),
            List.of(conceptTableVariable),
            subQueryFilterVariable);
    return query.renderSQL();
  }
}
