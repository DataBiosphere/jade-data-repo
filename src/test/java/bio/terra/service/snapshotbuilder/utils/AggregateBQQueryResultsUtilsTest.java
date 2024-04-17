package bio.terra.service.snapshotbuilder.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import bio.terra.model.SnapshotBuilderConcept;
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
            List.of(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "domain_id")),
            Field.of("domain_id", StandardSQLTypeName.STRING));
    assertThat(
        "toDomainId converts table result to a string",
        AggregateBQQueryResultsUtils.toDomainId(row),
        equalTo("domain_id"));
  }

  @Test
  void toConcept() {
    var expected =
        new SnapshotBuilderConcept()
            .name("concept_name")
            .id(1)
            .hasChildren(true)
            .count(100)
            .code(99L);
    FieldValueList row =
        FieldValueList.of(
            List.of(
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, String.valueOf(expected.getId())),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, expected.getName()),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, String.valueOf(expected.getCode())),
                FieldValue.of(
                    FieldValue.Attribute.PRIMITIVE, String.valueOf(expected.isHasChildren())),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, String.valueOf(expected.getCount()))),
            Field.of("concept_id", StandardSQLTypeName.NUMERIC),
            Field.of("concept_name", StandardSQLTypeName.STRING),
            Field.of("concept_code", StandardSQLTypeName.NUMERIC),
            Field.of("has_children", StandardSQLTypeName.BOOL),
            Field.of("count", StandardSQLTypeName.NUMERIC));
    assertThat(AggregateBQQueryResultsUtils.toConcept(row), is(expected));
  }
}
