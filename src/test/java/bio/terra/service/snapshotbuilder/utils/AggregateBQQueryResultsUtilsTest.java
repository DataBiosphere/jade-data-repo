package bio.terra.service.snapshotbuilder.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.service.snapshotbuilder.query.tables.Concept;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.StandardSQLTypeName;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class AggregateBQQueryResultsUtilsTest {
  @Test
  void toCount() {
    var values = FieldValueList.of(List.of(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "5")));

    assertThat(
        "mapper converts FieldValueList to int",
        AggregateBQQueryResultsUtils.toCount(values),
        equalTo(5));
  }

  @Test
  void toDomainId() {
    FieldValueList row =
        FieldValueList.of(
            List.of(FieldValue.of(FieldValue.Attribute.PRIMITIVE, Concept.DOMAIN_ID)),
            Field.of(Concept.DOMAIN_ID, StandardSQLTypeName.STRING));
    assertThat(
        "toDomainId converts table result to a string",
        AggregateBQQueryResultsUtils.toDomainId(row),
        equalTo(Concept.DOMAIN_ID));
  }

  @Test
  void toConcept() {
    var expected =
        new SnapshotBuilderConcept()
            .name(Concept.CONCEPT_NAME)
            .id(1)
            .hasChildren(true)
            .count(100)
            .code("99");
    FieldValueList row =
        FieldValueList.of(
            List.of(
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, String.valueOf(expected.getId())),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, expected.getName()),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, expected.getCode()),
                FieldValue.of(
                    FieldValue.Attribute.PRIMITIVE, String.valueOf(expected.isHasChildren())),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, String.valueOf(expected.getCount()))),
            Field.of(Concept.CONCEPT_ID, StandardSQLTypeName.NUMERIC),
            Field.of(Concept.CONCEPT_NAME, StandardSQLTypeName.STRING),
            Field.of(Concept.CONCEPT_CODE, StandardSQLTypeName.STRING),
            Field.of(QueryBuilderFactory.HAS_CHILDREN, StandardSQLTypeName.BOOL),
            Field.of(QueryBuilderFactory.COUNT, StandardSQLTypeName.NUMERIC));
    assertThat(AggregateBQQueryResultsUtils.toConcept(row), is(expected));
  }
}
