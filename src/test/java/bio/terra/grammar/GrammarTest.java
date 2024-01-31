package bio.terra.grammar;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToCompressingWhiteSpace;

import bio.terra.common.PdaoConstant;
import bio.terra.common.category.Unit;
import bio.terra.grammar.azure.SynapseVisitor;
import bio.terra.grammar.exception.InvalidQueryException;
import bio.terra.grammar.exception.MissingDatasetException;
import bio.terra.grammar.google.BigQueryVisitor;
import bio.terra.model.DatasetModel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class GrammarTest {

  private Map<String, DatasetModel> datasetMap;

  @Before
  public void setup() {
    DatasetModel datasetModel = new DatasetModel().dataProject("a-data-project");
    datasetMap = new HashMap<>();
    datasetMap.put("dataset", datasetModel);
    datasetMap.put("foo", datasetModel);
    datasetMap.put("baz", datasetModel);
  }

  @Test
  public void testDatasetNames() {
    Query query = Query.parse("SELECT * FROM foo.bar, baz.quux WHERE foo.bar.x = baz.quux.y");
    List<String> datasetNames = query.getDatasetNames();
    assertThat("there are two datasets", datasetNames.size(), equalTo(2));
    assertThat("it found the right datasets", datasetNames, hasItems("foo", "baz"));
  }

  @Test
  public void testTableNames() {
    Query query =
        Query.parse(
            "SELECT foo.bar.datarepo_row_id FROM foo.bar, baz.quux WHERE foo.bar.x = baz.quux.y");
    List<String> tableNames = query.getTableNames();
    assertThat("there are two tables", tableNames.size(), equalTo(2));
    assertThat("it found the right tables", tableNames, hasItems("bar", "quux"));
  }

  @Test
  public void testColumnNames() {
    Query query =
        Query.parse(
            "SELECT foo.bar.datarepo_row_id FROM foo.bar, baz.quux WHERE foo.bar.x = baz.quux.y");
    List<String> columnNames = query.getColumnNames();
    assertThat("there are three columns", columnNames.size(), equalTo(3));
    assertThat("it found the right columns", columnNames, hasItems("datarepo_row_id", "x", "y"));
  }

  @Test
  public void testColumnNamesOrder() {
    Query query =
        Query.parse(
            "SELECT baz.quux.datarepo_row_id FROM foo.bar, baz.quux WHERE foo.bar.x = baz.quux.y");
    List<String> columnNames = query.getColumnNames();
    assertThat("there are three columns", columnNames.size(), equalTo(3));
    assertThat("it found the right columns", columnNames, hasItems("datarepo_row_id", "x", "y"));
  }

  @Test
  public void testJoin() {
    Query query =
        Query.parse(
            "SELECT baz.quux.datarepo_row_id FROM foo.bar JOIN baz.quux ON foo.bar.x = baz.quux.y");
    List<String> columnNames = query.getColumnNames();
    assertThat("there are three columns", columnNames.size(), equalTo(3));
    assertThat("it found the right columns", columnNames, hasItems("datarepo_row_id", "x", "y"));
  }

  @Test
  public void testJoinWhere() {
    Query query =
        Query.parse(
            "SELECT foo.quux.datarepo_row_id FROM foo.bar JOIN foo.quux ON foo.bar.x = foo.quux.x WHERE foo.quux.z IN ('box')");
    List<String> columnNames = query.getColumnNames();
    assertThat("there are three columns", columnNames.size(), equalTo(3));
    assertThat("it found the right columns", columnNames, hasItems("datarepo_row_id", "x", "z"));
  }

  @Test
  public void testWhere() {
    Query query =
        Query.parse(
            "SELECT baz.quux.datarepo_row_id FROM foo.bar, baz.quux WHERE baz.quux.x IN ('lux') AND baz.quux.y IN ('box') ");
    List<String> columnNames = query.getColumnNames();
    assertThat("there are three columns", columnNames.size(), equalTo(3));
    assertThat("it found the right columns", columnNames, hasItems("datarepo_row_id", "x", "y"));
  }

  @Test
  public void testWhereLike() {
    Query query =
        Query.parse(
            "SELECT baz.quux.datarepo_row_id FROM foo.bar, baz.quux "
                + "WHERE baz.quux.x LIKE 'lux' AND baz.quux.y NOT LIKE 'box'");
    List<String> columnNames = query.getColumnNames();
    assertThat("there are three columns", columnNames.size(), equalTo(3));
    assertThat("it found the right columns", columnNames, hasItems("datarepo_row_id", "x", "y"));
  }

  @Test
  public void testBetween() {
    // test for DR-2703 Fix ANTLR error when parsing between clause
    Query.parse("SELECT foo.bar.datarepo_row_id FROM foo.bar WHERE foo.bar.x BETWEEN 1 and 2");
  }

  @Test
  public void testJSONDataInStringField() {
    Query.parse(
        "SELECT * FROM foo.bar WHERE CAST(JSON_EXTRACT_SCALAR(Description, '$.favoriteNumber') AS INT64) > 5");
  }

  @Test
  public void test1000Genomes() {
    // test for DR-2143 Fix validating dataset names that start with a number
    Query.parse("SELECT * FROM 1000GenomesDataset.sample_info");
  }

  @Test
  public void testColumnNotFullyQualifiedName() {
    // allow non-fully qualified column names to support preview filtering
    Query.parse("SELECT datarepo_row_id FROM foo.bar, baz.quux WHERE foo.bar.x = baz.quux.y");
    Query.parse("SELECT * FROM snapshot.table WHERE column = val");
  }

  @Test
  public void testSynapseTranslate() {
    String sourceDatasetDataSourceName = "sourceDatasetDataSourceName1";
    SynapseVisitor synapseVisitor = new SynapseVisitor(datasetMap, sourceDatasetDataSourceName);
    Query query =
        Query.parse(
            "SELECT vocabulary.datarepo_row_id FROM datasetName.vocabulary WHERE vocabulary.vocabulary_id IN ('1')");
    String expectedQuery =
        """
    SELECT alias927641339.datarepo_row_id FROM (SELECT * FROM
    OPENROWSET(
      BULK 'metadata/parquet/vocabulary/*/*.parquet',
      DATA_SOURCE = 'sourceDatasetDataSourceName1',
      FORMAT = 'parquet') AS inner_alias927641339) AS alias927641339
    WHERE alias927641339.vocabulary_id IN ( '1' )""";
    assertThat(
        "Translation is correct",
        query.translateSql(synapseVisitor),
        equalToCompressingWhiteSpace(expectedQuery));
  }

  @Test
  public void testSynapseTranslate_NoTableNameAlias() {
    String sourceDatasetDataSourceName = "sourceDatasetDataSourceName1";
    SynapseVisitor synapseVisitor = new SynapseVisitor(datasetMap, sourceDatasetDataSourceName);
    Query query =
        Query.parse(
            "SELECT datarepo_row_id FROM datasetName.vocabulary WHERE vocabulary_id IN ('1')");
    String expectedQuery =
        """
      SELECT datarepo_row_id FROM (SELECT * FROM OPENROWSET(
        BULK 'metadata/parquet/vocabulary/*/*.parquet',
        DATA_SOURCE = 'sourceDatasetDataSourceName1',
        FORMAT = 'parquet') AS inner_alias927641339) AS alias927641339
      WHERE vocabulary_id IN ( '1' )""";
    assertThat(
        "Translation is correct",
        query.translateSql(synapseVisitor),
        equalToCompressingWhiteSpace(expectedQuery));
  }

  @Test
  public void testSynapseTranslate_UIQuery() {
    String sourceDatasetDataSourceName = "sourceDatasetDataSourceName1";
    SynapseVisitor synapseVisitor = new SynapseVisitor(datasetMap, sourceDatasetDataSourceName);
    Query query =
        Query.parse(
            "SELECT it_dataset_omop3e3960eb_a12c_441b_ac07_d863f1bce90b.vocabulary.datarepo_row_id FROM it_dataset_omop3e3960eb_a12c_441b_ac07_d863f1bce90b.vocabulary WHERE (it_dataset_omop3e3960eb_a12c_441b_ac07_d863f1bce90b.vocabulary.vocabulary_id IN (\"1\"))");
    String expectedQuery =
        """
     SELECT alias927641339.datarepo_row_id FROM (SELECT * FROM
     OPENROWSET(
       BULK 'metadata/parquet/vocabulary/*/*.parquet',
       DATA_SOURCE = 'sourceDatasetDataSourceName1',
       FORMAT = 'parquet') AS inner_alias927641339) AS alias927641339
     WHERE ( alias927641339.vocabulary_id IN ( '1' ) )""";
    assertThat(
        "Translation is correct",
        query.translateSql(synapseVisitor),
        Matchers.equalToCompressingWhiteSpace(expectedQuery));
  }

  @Test
  public void testSynapseTranslate_UIQueryWithJoin() {
    String userQuery =
        "SELECT Azure_V2F_GWAS_Summary_Statistics.variant.datarepo_row_id FROM Azure_V2F_GWAS_Summary_Statistics.variant JOIN Azure_V2F_GWAS_Summary_Statistics.ancestry_specific_meta_analysis ON Azure_V2F_GWAS_Summary_Statistics.ancestry_specific_meta_analysis.variant_id = Azure_V2F_GWAS_Summary_Statistics.variant.id WHERE Azure_V2F_GWAS_Summary_Statistics.ancestry_specific_meta_analysis.variant_id  IN (\"1:104535993:T:C\")";
    String sourceDatasetDataSourceName = "sourceDatasetDataSourceName1";
    SynapseVisitor synapseVisitor = new SynapseVisitor(datasetMap, sourceDatasetDataSourceName);
    Query query = Query.parse(userQuery);
    String expectedQuery =
        """
    SELECT alias236785828.datarepo_row_id FROM (SELECT * FROM
    OPENROWSET(
       BULK 'metadata/parquet/variant/*/*.parquet',
       DATA_SOURCE = 'sourceDatasetDataSourceName1',
       FORMAT = 'parquet') AS inner_alias236785828)
     AS alias236785828
     JOIN (SELECT * FROM
      OPENROWSET(
        BULK 'metadata/parquet/ancestry_specific_meta_analysis/*/*.parquet',
        DATA_SOURCE = 'sourceDatasetDataSourceName1',
        FORMAT = 'parquet') AS inner_alias1748223664)
     AS alias1748223664
     ON alias1748223664.variant_id = alias236785828.id WHERE alias1748223664.variant_id IN ( '1:104535993:T:C' )""";
    assertThat(
        "Translation is correct",
        query.translateSql(synapseVisitor),
        equalToCompressingWhiteSpace(expectedQuery));
  }

  @Test
  public void testBqTranslate() {
    BigQueryVisitor bqVisitor = new BigQueryVisitor(datasetMap);
    String bqDatasetName = PdaoConstant.PDAO_PREFIX + "dataset";
    String tableName = "table";
    String explicitTableName =
        String.join(".", datasetMap.get("dataset").getDataProject(), bqDatasetName, tableName);
    Query query =
        Query.parse(
            "SELECT dataset.table.datarepo_row_id FROM dataset.table WHERE dataset.table.x = 'string'");
    String translated = query.translateSql(bqVisitor);
    String aliasedTableName = bqVisitor.generateAlias(bqDatasetName, tableName);
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

  @Test
  public void testBqTranslateTwoTables() {
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
    String aliasedTable1Name = bqVisitor.generateAlias(bqDataset1Name, table1Name);
    String aliasedTable2Name = bqVisitor.generateAlias(bqDataset2Name, table2Name);
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
  public void testBqTranslateTwoTablesOrdered() {
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
    String aliasedTable1Name = bqVisitor.generateAlias(bqDataset1Name, table1Name);
    String aliasedTable2Name = bqVisitor.generateAlias(bqDataset2Name, table2Name);
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

  @Test(expected = InvalidQueryException.class)
  public void testInvalidQuery() {
    Query query = Query.parse("this is not a valid query");
    query.getDatasetNames();
  }

  @Test(expected = MissingDatasetException.class)
  public void testInvalidDataset() {
    BigQueryVisitor bqVisitor = new BigQueryVisitor(datasetMap);
    Query query = Query.parse("SELECT * FROM noDataset.table WHERE noDataset.table.x = 'string'");
    query.translateSql(bqVisitor);
  }

  @Test(expected = InvalidQueryException.class)
  public void testDeleteInvalid() {
    Query query = Query.parse("DELETE * FROM foo.bar");
    query.getDatasetNames();
  }

  @Test(expected = InvalidQueryException.class)
  public void testSemicolonsInvalid() {
    Query query = Query.parse("SELECT * FROM foo.bar; DELETE * FROM foo.bar");
    query.getDatasetNames();
  }

  @Test(expected = InvalidQueryException.class)
  public void testUpdateInvalid() {
    Query query = Query.parse("INSERT INTO foo.bar (x, y, z) VALUES 1 2 3");
    query.getDatasetNames();
  }

  @Test
  public void testNestedQuery() {
    Query.parse(
        """
          SELECT datarepo_row_id, file_id, sample_id, sample_vcf FROM ( SELECT datarepo_row_id, file_id, sample_id, sample_vcf FROM DATABASE.TABLE ) """);
  }

  @Test
  public void testOverClause() {
    Query.parse(
        """
            SELECT datarepo_row_id, file_id, sample_id, sample_vcf, total_row_count, count(*) over() as filtered_row_count FROM (
                          Select datarepo_row_id, file_id, sample_id, sample_vcf, count(*) over () as total_row_count FROM datarepo_test_v2_drs_anvil_v3.file
                        ) WHERE file_id = 'TEST_File_002' LIMIT 1000
            """);
  }
}
