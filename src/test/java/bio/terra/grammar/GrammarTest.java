package bio.terra.grammar;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.PdaoConstant;
import bio.terra.common.category.Unit;
import bio.terra.grammar.exception.InvalidQueryException;
import bio.terra.grammar.exception.MissingDatasetException;
import bio.terra.grammar.google.BigQueryVisitor;
import bio.terra.model.DatasetModel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
}
