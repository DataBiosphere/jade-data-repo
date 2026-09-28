package bio.terra.grammar;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.PdaoConstant;
import bio.terra.common.category.Unit;
import bio.terra.grammar.exception.InvalidQueryException;
import bio.terra.grammar.exception.MissingDatasetException;
import bio.terra.grammar.google.BigQueryVisitor;
import bio.terra.model.DatasetModel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

@Tag(Unit.TAG)
class GrammarTest {

  private Map<String, DatasetModel> datasetMap;

  @BeforeEach
  void setup() {
    DatasetModel datasetModel = new DatasetModel().dataProject("a-data-project");
    datasetMap = new HashMap<>();
    datasetMap.put("dataset", datasetModel);
    datasetMap.put("foo", datasetModel);
    datasetMap.put("baz", datasetModel);
  }

  @Test
  void testDatasetNames() {
    Query query = Query.parse("SELECT * FROM foo.bar, baz.quux WHERE foo.bar.x = baz.quux.y");
    List<String> datasetNames = query.getDatasetNames();
    assertThat("there are two datasets", datasetNames.size(), equalTo(2));
    assertThat("it found the right datasets", datasetNames, hasItems("foo", "baz"));
  }

  @Test
  void testTableNames() {
    Query query =
        Query.parse(
            "SELECT foo.bar.datarepo_row_id FROM foo.bar, baz.quux WHERE foo.bar.x = baz.quux.y");
    List<String> tableNames = query.getTableNames();
    assertThat("it found the right tables", tableNames, containsInAnyOrder("bar", "quux"));
  }

  @Test
  void testTableNamesKeywordUnquoted() {
    assertThrows(
        InvalidQueryException.class,
        () ->
            Query.parse(
                "SELECT foo.interval.datarepo_row_id FROM foo.interval, baz.quux WHERE foo.interval.x = baz.quux.y"),
        "Table name 'interval' is a reserved keyword and must be quoted with double quotes.");
  }

  @Test
  void testTableNamesKeywordQuoted() {
    Query query =
        Query.parse(
            "SELECT foo.\"interval\".datarepo_row_id FROM foo.\"interval\", baz.quux WHERE foo.\"interval\".x = baz.quux.y");
    List<String> tableNames = query.getTableNames();
    assertThat("it found the right tables", tableNames, containsInAnyOrder("\"interval\"", "quux"));
  }

  static Stream<Arguments> testColumnNames() {
    return Stream.of(
        Arguments.of(
            "SELECT foo.bar.datarepo_row_id FROM foo.bar, baz.quux WHERE foo.bar.x = baz.quux.y",
            List.of("datarepo_row_id", "x", "y")),
        Arguments.of(
            "SELECT baz.quux.datarepo_row_id FROM foo.bar, baz.quux WHERE foo.bar.x = baz.quux.y",
            List.of("datarepo_row_id", "x", "y")),
        Arguments.of(
            "SELECT baz.quux.datarepo_row_id FROM foo.bar JOIN baz.quux ON foo.bar.x = baz.quux.y",
            List.of("datarepo_row_id", "x", "y")),
        Arguments.of(
            "SELECT foo.quux.datarepo_row_id FROM foo.bar JOIN foo.quux ON foo.bar.x = foo.quux.x WHERE foo.quux.z IN ('box')",
            List.of("datarepo_row_id", "x", "z")),
        Arguments.of(
            "SELECT baz.quux.datarepo_row_id FROM foo.bar, baz.quux WHERE baz.quux.x IN ('lux') AND baz.quux.y IN ('box') ",
            List.of("datarepo_row_id", "x", "y")),
        Arguments.of(
            "SELECT baz.quux.datarepo_row_id FROM foo.bar, baz.quux "
                + "WHERE baz.quux.x LIKE 'lux' AND baz.quux.y NOT LIKE 'box'",
            List.of("datarepo_row_id", "x", "y")),
        // test for DR-2703 Fix ANTLR error when parsing between clause
        Arguments.of(
            "SELECT foo.bar.datarepo_row_id FROM foo.bar WHERE foo.bar.x BETWEEN 1 and 2",
            List.of("datarepo_row_id", "x")));
  }

  @ParameterizedTest
  @MethodSource
  void testColumnNames(String sql, List<String> expectedColumns) {
    assertThat(
        "it found the right columns",
        Query.parse(sql).getColumnNames(),
        containsInAnyOrder(expectedColumns.toArray()));
  }

  @Test
  void testJSONDataInStringField() {
    Query query =
        Query.parse(
            "SELECT * FROM foo.bar WHERE CAST(JSON_EXTRACT_SCALAR(Description, '$.favoriteNumber') AS INT64) > 5");
    assertThat(query.getTableNames(), containsInAnyOrder("bar"));
  }

  @Test
  void test1000Genomes() {
    // test for DR-2143 Fix validating dataset names that start with a number
    Query query = Query.parse("SELECT * FROM 1000GenomesDataset.sample_info");
    assertThat(query.getDatasetNames(), containsInAnyOrder("1000GenomesDataset"));
  }

  @Test
  void testColumnNotFullyQualifiedName() {
    // allow non-fully qualified column names to support preview filtering
    Query query =
        Query.parse("SELECT datarepo_row_id FROM foo.bar, baz.quux WHERE foo.bar.x = baz.quux.y");
    assertThat(query.getColumnNames(), containsInAnyOrder("datarepo_row_id", "x", "y"));
    query = Query.parse("SELECT * FROM snapshot.table WHERE column = val");
    assertThat(query.getColumnNames(), containsInAnyOrder("column", "val"));
  }

  @Test
  void testBqTranslate() {
    BigQueryVisitor bqVisitor = new BigQueryVisitor(datasetMap);
    String bqDatasetName = PdaoConstant.PDAO_PREFIX + "dataset";
    String tableName = "table";
    String explicitTableName =
        String.join(".", datasetMap.get("dataset").getDataProject(), bqDatasetName, tableName);
    Query query =
        Query.parse(
            "SELECT dataset.table.datarepo_row_id FROM dataset.table WHERE dataset.table.x = 'string'");
    String translated = query.translateSql(bqVisitor);
    String aliasedTableName = BigQueryVisitor.generateAlias(bqDatasetName, tableName);
    assertThat(
        "query translates to valid bigquery syntax",
        translated,
        equalTo(
            "SELECT `"
                + aliasedTableName
                + "`.datarepo_row_id FROM `"
                + explicitTableName
                + "` "
                + "AS `"
                + aliasedTableName
                + "` WHERE `"
                + aliasedTableName
                + "`.x = 'string'"));
  }

  @ParameterizedTest
  @MethodSource
  void testRequiredQualifiedNameOnQueryTranslation(
      String testQuery, boolean queryHasQualifiedNames) {
    BigQueryVisitor bqVisitor = new BigQueryVisitor(datasetMap);
    Query parsedQuery = Query.parse(testQuery);
    if (queryHasQualifiedNames) {
      String bqDatasetName = PdaoConstant.PDAO_PREFIX + "dataset";
      String tableName = "table";
      String translated = parsedQuery.translateSql(bqVisitor);
      String aliasedTableName = BigQueryVisitor.generateAlias(bqDatasetName, tableName);
      assertThat(
          "query translates to valid bigquery syntax",
          translated,
          containsString(aliasedTableName));
    } else {
      assertThrows(
          InvalidQueryException.class,
          () -> parsedQuery.translateSql(bqVisitor),
          "All column names must be qualified with a dataset and table name. ");
    }
  }

  static Stream<Arguments> testRequiredQualifiedNameOnQueryTranslation() {
    return Stream.of(
        Arguments.of(
            "SELECT dataset.table.datarepo_row_id FROM dataset.table WHERE dataset.table.x = 'string'",
            true),
        Arguments.of(
            "SELECT datarepo_row_id FROM dataset.table WHERE dataset.table.x = 'string'", false),
        Arguments.of("SELECT * FROM dataset.table WHERE dataset.table.x = 'string'", true),
        Arguments.of("SELECT datarepo_row_id FROM dataset.table WHERE x = 'string'", false));
  }

  @Test
  void testRequiredQualifiedDatasetName() {
    assertThrows(
        InvalidQueryException.class,
        () ->
            Query.parse(
                "SELECT dataset.table.datarepo_row_id FROM table WHERE dataset.table.x = 'string'"),
        "All table names must be qualified with a dataset name.");
  }

  @Test
  void testBqTranslateTwoTables() {
    BigQueryVisitor bqVisitor = new BigQueryVisitor(datasetMap);
    String bqDataset1Name = PdaoConstant.PDAO_PREFIX + "foo";
    String bqDataset2Name = PdaoConstant.PDAO_PREFIX + "baz";
    String table1Name = "bar";
    String table2Name = "quux";
    String explicitTable1Name =
        String.join(".", datasetMap.get("foo").getDataProject(), bqDataset1Name, table1Name);
    String explicitTable2Name =
        String.join(".", datasetMap.get("baz").getDataProject(), bqDataset2Name, table2Name);
    Query query =
        Query.parse(
            "SELECT foo.bar.datarepo_row_id FROM foo.bar, baz.quux WHERE foo.bar.x = baz.quux.y");
    String translated = query.translateSql(bqVisitor);
    String aliasedTable1Name = BigQueryVisitor.generateAlias(bqDataset1Name, table1Name);
    String aliasedTable2Name = BigQueryVisitor.generateAlias(bqDataset2Name, table2Name);
    assertThat(
        "query translates to valid bigquery syntax",
        translated,
        equalTo(
            "SELECT `"
                + aliasedTable1Name
                + "`.datarepo_row_id "
                + "FROM `"
                + explicitTable1Name
                + "` AS `"
                + aliasedTable1Name
                + "` ,"
                + " `"
                + explicitTable2Name
                + "` AS `"
                + aliasedTable2Name
                + "` "
                + "WHERE `"
                + aliasedTable1Name
                + "`.x = `"
                + aliasedTable2Name
                + "`.y"));
  }

  @Test
  void testBqTranslateTwoTablesOrdered() {
    BigQueryVisitor bqVisitor = new BigQueryVisitor(datasetMap);
    String bqDataset1Name = PdaoConstant.PDAO_PREFIX + "foo";
    String bqDataset2Name = PdaoConstant.PDAO_PREFIX + "baz";
    String table1Name = "bar";
    String table2Name = "quux";
    String explicitTable1Name =
        String.join(".", datasetMap.get("foo").getDataProject(), bqDataset1Name, table1Name);
    String explicitTable2Name =
        String.join(".", datasetMap.get("baz").getDataProject(), bqDataset2Name, table2Name);
    Query query =
        Query.parse(
            "SELECT baz.quux.datarepo_row_id FROM foo.bar, baz.quux WHERE foo.bar.x = baz.quux.y");
    String translated = query.translateSql(bqVisitor);
    String aliasedTable1Name = BigQueryVisitor.generateAlias(bqDataset1Name, table1Name);
    String aliasedTable2Name = BigQueryVisitor.generateAlias(bqDataset2Name, table2Name);
    assertThat(
        "query translates to valid bigquery syntax",
        translated,
        equalTo(
            "SELECT `"
                + aliasedTable2Name
                + "`.datarepo_row_id "
                + "FROM `"
                + explicitTable1Name
                + "` AS `"
                + aliasedTable1Name
                + "` ,"
                + " `"
                + explicitTable2Name
                + "` AS `"
                + aliasedTable2Name
                + "` "
                + "WHERE `"
                + aliasedTable1Name
                + "`.x = `"
                + aliasedTable2Name
                + "`.y"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "this is not a valid query",
        "DELETE * FROM foo.bar",
        "SELECT * FROM foo.bar; DELETE * FROM foo.bar",
        "INSERT INTO foo.bar (x, y, z) VALUES 1 2 3"
      })
  void testInvalidQuery(String sql) {
    assertThrows(InvalidQueryException.class, () -> Query.parse(sql));
  }

  @Test
  void testInvalidDataset() {
    BigQueryVisitor bqVisitor = new BigQueryVisitor(datasetMap);
    Query query = Query.parse("SELECT * FROM noDataset.table WHERE noDataset.table.x = 'string'");
    assertThrows(MissingDatasetException.class, () -> query.translateSql(bqVisitor));
  }

  @Test
  void testNestedQuery() {
    Query query =
        Query.parse(
            """
            SELECT datarepo_row_id, file_id, sample_id, sample_vcf FROM ( SELECT datarepo_row_id, file_id, sample_id, sample_vcf FROM DATABASE.TABLE )""");
    assertThat(query.getDatasetNames(), hasItems("DATABASE"));
  }

  @Test
  void testOverClause() {
    Query query =
        Query.parse(
            """
            SELECT datarepo_row_id, file_id, sample_id, sample_vcf, total_row_count, count(*) over() as filtered_row_count FROM (
                          Select datarepo_row_id, file_id, sample_id, sample_vcf, count(*) over () as total_row_count FROM datarepo_test_v2_drs_anvil_v3.file
                        ) WHERE file_id = 'TEST_File_002' LIMIT 1000
            """);
    assertThat(query.getDatasetNames(), hasItems("datarepo_test_v2_drs_anvil_v3"));
  }
}
