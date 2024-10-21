package bio.terra.service.dataset;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.Relationship;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.DatasetFixtures;
import bio.terra.model.AssetModel;
import bio.terra.model.TableDataType;
import bio.terra.service.dataset.exception.InvalidAssetException;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class ValidateAssetUnitTest {

  private static final String TABLE_NAME = "tableName1";
  private static final String TABLE_NAME_2 = "tableName2";
  private static final String COL_1_NAME = "col1";
  private static final String COL_2_NAME = "col2";
  private static final String COL_3_NAME = "col3";
  private static final String RELATIONSHIP_NAME = "rel1";
  private Dataset dataset;
  private AssetModel assetModel;

  @BeforeEach
  void setup() {
    dataset =
        new Dataset()
            .tables(
                List.of(
                    DatasetFixtures.generateDatasetTable(
                        TABLE_NAME, TableDataType.STRING, List.of(COL_1_NAME, COL_2_NAME)),
                    DatasetFixtures.generateDatasetTable(
                        TABLE_NAME_2, TableDataType.STRING, List.of(COL_3_NAME))))
            .relationships(List.of(new Relationship().name(RELATIONSHIP_NAME)));

    assetModel =
        new AssetModel()
            .name("assetName")
            .rootTable(TABLE_NAME)
            .rootColumn(COL_1_NAME)
            .tables(
                List.of(
                    DatasetFixtures.generateAssetTable(
                        TABLE_NAME, List.of(COL_1_NAME, COL_2_NAME))))
            .follow(List.of(RELATIONSHIP_NAME));
  }

  @Test
  void validateAssetModel() {
    dataset.validateDatasetAssetSpecification(assetModel);
  }

  @Test
  void testTwoTables() {
    assetModel.tables(
        List.of(
            DatasetFixtures.generateAssetTable(TABLE_NAME, List.of(COL_1_NAME, COL_2_NAME)),
            DatasetFixtures.generateAssetTable(TABLE_NAME_2, List.of(COL_3_NAME))));
    dataset.validateDatasetAssetSpecification(assetModel);
  }

  @Test
  void testTwoTablesInvalidOverlap() {
    // col3 is only in table2, not table2
    assetModel.tables(
        List.of(
            DatasetFixtures.generateAssetTable(TABLE_NAME, List.of(COL_1_NAME, COL_3_NAME)),
            DatasetFixtures.generateAssetTable(TABLE_NAME_2, List.of(COL_3_NAME))));
    testAssetModel(
        "invalid column", "Column " + COL_3_NAME + " does not exist in table " + TABLE_NAME);
  }

  @Test
  void testNoFollow() {
    assetModel.follow(null);
    dataset.validateDatasetAssetSpecification(assetModel);
  }

  @Test
  void testEmptyFollowList() {
    assetModel.follow(Collections.emptyList());
    dataset.validateDatasetAssetSpecification(assetModel);
  }

  @Test
  void testInvalidColumn() {
    String invalidColumn = "InvalidCol";
    assetModel.tables(
        List.of(
            DatasetFixtures.generateAssetTable(TABLE_NAME, List.of(COL_1_NAME, invalidColumn))));
    testAssetModel(
        "invalid column", "Column " + invalidColumn + " does not exist in table " + TABLE_NAME);
  }

  @Test
  void testInvalidTable() {
    String invalidTable = "invalidTable";
    assetModel.tables(
        List.of(DatasetFixtures.generateAssetTable(invalidTable, List.of(COL_1_NAME, COL_2_NAME))));
    testAssetModel("invalid table", "Table " + invalidTable + " does not exist in dataset.");
  }

  @Test
  void testInvalidRootTable() {
    String invalidRootTable = "InvalidRootTable";
    assetModel.rootTable(invalidRootTable);
    testAssetModel(
        "invalid root table", "Root table " + invalidRootTable + " does not exist in dataset.");
  }

  @Test
  void testInvalidRootColumn() {
    String invalidRootColumn = "InvalidRootColumn";
    assetModel.rootColumn(invalidRootColumn);
    testAssetModel(
        "invalid root column",
        "Root column " + invalidRootColumn + " does not exist in table " + TABLE_NAME);
  }

  @Test
  void testInvalidFollowRelationship() {
    String invalidFollowRelationship = "InvalidRelationship";
    assetModel.follow(List.of(RELATIONSHIP_NAME, invalidFollowRelationship));
    testAssetModel(
        "invalid relationship",
        "Relationship specified in follow list '"
            + invalidFollowRelationship
            + "' does not exist in dataset's list of relationships");
  }

  @Test
  void testMultipleErrors() {
    String invalidRootTable = "InvalidRootTable";
    assetModel.rootTable(invalidRootTable);
    String invalidColumn = "InvalidCol";
    String invalidColumn2 = "InvalidCol2";
    assetModel.tables(
        List.of(
            DatasetFixtures.generateAssetTable(
                TABLE_NAME, List.of(invalidColumn, invalidColumn2))));
    InvalidAssetException ex =
        assertThrows(
            InvalidAssetException.class,
            () -> dataset.validateDatasetAssetSpecification(assetModel));
    assertThat(
        "At least one validation error caught for asset",
        ex.getMessage(),
        containsString("Invalid asset create request. See causes list for details."));
    assertThat("3 errors found", ex.getCauses(), hasSize(3));
    assertThat(
        "Validation of asset model should fail on invalid root table",
        ex.getCauses().get(0),
        containsString("Root table " + invalidRootTable + " does not exist in dataset."));
    assertThat(
        "Validation of asset model should fail on invalid column",
        ex.getCauses().get(1),
        containsString("Column " + invalidColumn + " does not exist in table " + TABLE_NAME));
    assertThat(
        "Validation of asset model should fail on second invalid column",
        ex.getCauses().get(2),
        containsString("Column " + invalidColumn2 + " does not exist in table " + TABLE_NAME));
  }

  private void testAssetModel(String itemChecked, String expectedErrorMessage) {
    InvalidAssetException ex =
        assertThrows(
            InvalidAssetException.class,
            () -> dataset.validateDatasetAssetSpecification(assetModel));
    assertThat(
        "At least one validation error caught for asset",
        ex.getMessage(),
        containsString("Invalid asset create request. See causes list for details."));
    assertThat(
        "Validation of asset model should fail on " + itemChecked,
        ex.getCauses().get(0),
        containsString(expectedErrorMessage));
  }
}
