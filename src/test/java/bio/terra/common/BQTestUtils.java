package bio.terra.common;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import bio.terra.service.tabulardata.google.BigQueryProject;
import com.google.cloud.PageImpl;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValue.Attribute;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.mockito.stubbing.Answer;

/** Helper methods for mocking out BigQuery responses */
public final class BQTestUtils {

  private BQTestUtils() {}

  public static void mockBQQuery(
      BigQueryProject mockBQProject, String sql, Schema schema, List<Map<String, String>> results) {
    try {
      when(mockBQProject.query(sql)).thenAnswer(mockAnswer(schema, results));
    } catch (InterruptedException e) {
      throw new RuntimeException("Error mocking query execution", e);
    }
  }

  public static void mockBQQueryWithArgs(
      BigQueryProject mockBQProject, String sql, Schema schema, List<Map<String, String>> results) {
    try {
      when(mockBQProject.query(eq(sql), any())).thenAnswer(mockAnswer(schema, results));
    } catch (InterruptedException e) {
      throw new RuntimeException("Error mocking query execution", e);
    }
  }

  public static void mockBQQueryError(BigQueryProject mockBQProject, String sql, Throwable ex) {
    try {
      when(mockBQProject.query(sql)).thenThrow(ex);
    } catch (InterruptedException e) {
      throw new RuntimeException("Error mocking query execution", e);
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
      throw new RuntimeException("Error mocking query execution", e);
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
                        .toList(),
                    schema.getFields()))
        .toList();
  }

  public static List<Map<String, Object>> mapToList(TableResult tableResult, String... fields) {
    List<Map<String, Object>> returnVal = new ArrayList<>();
    List<Field> fieldsToProcess =
        tableResult.getSchema().getFields().stream()
            .filter(f -> fields.length == 0 || List.of(fields).contains(f.getName()))
            .toList();
    tableResult.getValues().forEach(r -> returnVal.add(toMap(fieldsToProcess, r)));
    return returnVal;
  }

  // We previously used Collectors.toMap but its implementation does not permit null values.
  private static Map<String, Object> toMap(List<Field> fields, FieldValueList row) {
    Map<String, Object> returnVal = new HashMap<>();
    fields.forEach(f -> returnVal.put(f.getName(), mapFieldValueToPojo(f, row.get(f.getName()))));
    return returnVal;
  }

  private static Object mapFieldValueToPojo(Field field, FieldValue fieldValue) {
    if (fieldValue.isNull()) {
      return null;
    }
    return switch (field.getType().getStandardType()) {
      case BOOL -> extractData(fieldValue, FieldValue::getBooleanValue);
      case FLOAT64 -> extractData(fieldValue, FieldValue::getDoubleValue);
      case INT64 -> extractData(fieldValue, v -> Long.valueOf(v.getLongValue()).intValue());
      case NUMERIC -> extractData(fieldValue, FieldValue::getNumericValue);
      case STRING -> extractData(fieldValue, FieldValue::getStringValue);
      case TIMESTAMP -> extractData(fieldValue, FieldValue::getTimestampValue);
      default -> extractData(fieldValue, FieldValue::getValue);
    };
  }

  private static Object extractData(FieldValue fieldValue, Function<FieldValue, Object> extractor) {
    if (fieldValue.getAttribute() == Attribute.REPEATED) {
      return fieldValue.getRepeatedValue().stream().map(extractor).toList();
    }
    return extractor.apply(fieldValue);
  }
}
