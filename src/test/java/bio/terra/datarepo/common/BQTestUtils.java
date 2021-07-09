package bio.terra.common;

import static org.mockito.Mockito.when;

import bio.terra.service.tabulardata.google.BigQueryProject;
import com.google.cloud.PageImpl;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableResult;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.mockito.stubbing.Answer;

/** Helper methods for mocking out BigQuery responses */
public final class BQTestUtils {

  private BQTestUtils() {}

  public static void mockBQQuery(
      BigQueryProject mockBQProject, String sql, Schema schema, List<Map<String, String>> results) {
    try {
      when(mockBQProject.query(sql)).thenAnswer(mockAnswer(schema, results));
    } catch (InterruptedException e) {
      throw new RuntimeException("Error mocking query execution");
    }
  }

  public static void mockBQQuery(
      BigQuery mockBQ,
      QueryJobConfiguration sql,
      Schema schema,
      List<Map<String, String>> results) {
    try {
      when(mockBQ.query(sql)).thenAnswer(mockAnswer(schema, results));
    } catch (InterruptedException e) {
      throw new RuntimeException("Error mocking query execution");
    }
  }

  private static Answer<TableResult> mockAnswer(Schema schema, List<Map<String, String>> results) {
    return a ->
        new TableResult(
            schema, results.size(), new PageImpl<>(null, null, convertValues(results, schema)));
  }

  private static List<FieldValueList> convertValues(
      List<Map<String, String>> results, Schema schema) {
    // Note: using a little bit of a convoluted approach for constructing the values list but we
    // need
    // to maintain the order
    return results.stream()
        .map(
            r ->
                FieldValueList.of(
                    schema.getFields().stream()
                        .map(f -> FieldValue.of(FieldValue.Attribute.PRIMITIVE, r.get(f.getName())))
                        .collect(Collectors.toList()),
                    schema.getFields()))
        .collect(Collectors.toList());
  }
}
