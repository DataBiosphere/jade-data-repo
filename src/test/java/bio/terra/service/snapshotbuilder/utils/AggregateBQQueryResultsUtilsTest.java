package bio.terra.service.snapshotbuilder.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Unit;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class AggregateBQQueryResultsUtilsTest {
  @Test
  void rollupCountsReturnsListOfInt() {
    var values = FieldValueList.of(List.of(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "5")));

    assertThat(
        "mapper converts FieldValueList to int",
        AggregateBQQueryResultsUtils.toCount(values),
        equalTo(5));
  }

  @Test
  void domainIdReturnsListOfString() {
    TableResult table = makeTableResult("domain_id", "domain");
    assertThat(
        "domainId converts table result to list of String",
        AggregateBQQueryResultsUtils.domainId(table),
        equalTo(List.of("domain")));
  }

  private TableResult makeTableResult(String columnName, String value) {
    Schema schema = Schema.of(Field.of(columnName, StandardSQLTypeName.STRING));
    Page<FieldValueList> page =
        BigQueryPdaoUnitTest.mockPage(
            List.of(
                FieldValueList.of(List.of(FieldValue.of(FieldValue.Attribute.PRIMITIVE, value)))));

    return new TableResult(schema, 1, page);
  }
}
