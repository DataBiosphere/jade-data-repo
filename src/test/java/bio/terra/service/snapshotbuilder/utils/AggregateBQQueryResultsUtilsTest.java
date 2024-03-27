package bio.terra.service.snapshotbuilder.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Unit;
import bio.terra.service.tabulardata.google.bigquery.BigQueryPdaoUnitTest;
import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableResult;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class AggregateBQQueryResultsUtilsTest {
  @Test
  void rollupCountsReturnsListOfInt() {
    TableResult table = makeTableResult("count_name", "5");
    assertThat(
        "rollupCountsMapper converts table result to list of ints",
        AggregateBQQueryResultsUtils.rollupCountsMapper(table),
        equalTo(List.of(5)));
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
