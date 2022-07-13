package bio.terra.service.dataset.flight.update;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import bio.terra.common.Column;
import bio.terra.common.Relationship;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.DatasetFixtures;
import bio.terra.model.ColumnModel;
import bio.terra.model.DatasetSchemaColumnUpdateModel;
import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.model.DatasetSchemaUpdateModelChanges;
import bio.terra.model.RelationshipModel;
import bio.terra.model.RelationshipTermModel;
import bio.terra.model.TableDataType;
import bio.terra.model.TableModel;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.BigQueryPartitionConfigV1;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.job.JobService;
import bio.terra.service.load.LoadService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = {"datarepo.testWithEmbeddedDatabase=false"})
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class DatasetSchemaUpdateValidationTest {

  @MockBean private DatasetService datasetService;
  @MockBean private JobService jobService;

  @MockBean private LoadService loadService;

  @MockBean private ConfigurationService configurationService;

  private UUID datasetId;
  private static final String EXISTING_TABLE = "existing_table";
  private static final String EXISTING_COLUMN = "existing_column";
  private static final String EXISTING_TABLE_2 = "existing_table_2";
  private static final String EXISTING_COLUMN_2 = "existing_column_2";
  private static final String EXISTING_RELATIONSHIP = "existing_relationship";

  @Before
  public void setup() {
    datasetId = UUID.randomUUID();
    UUID existingTableId = UUID.randomUUID();
    UUID existingColumnId = UUID.randomUUID();
    BigQueryPartitionConfigV1 config = BigQueryPartitionConfigV1.none();
    DatasetTable table =
        new DatasetTable()
            .name(EXISTING_TABLE)
            .id(existingTableId)
            .columns(
                List.of(
                    new Column()
                        .name(EXISTING_COLUMN)
                        .id(existingColumnId)
                        .type(TableDataType.STRING)
                        .arrayOf(false)
                        .required(true)))
            .bigQueryPartitionConfig(config);
    DatasetTable table2 =
        DatasetFixtures.generateDatasetTable(
                EXISTING_TABLE_2, TableDataType.STRING, List.of(EXISTING_COLUMN_2))
            .bigQueryPartitionConfig(config);
    Relationship relationship =
        new Relationship()
            .name(EXISTING_RELATIONSHIP)
            .fromTable(table)
            .fromColumn(table.getColumns().get(0))
            .toTable(table2)
            .toColumn(table2.getColumns().get(0));
    when(datasetService.retrieve(datasetId))
        .thenReturn(
            new Dataset()
                .id(datasetId)
                .name("ValidationTestDataset")
                .description("A dataset to test schema update validation")
                .tables(List.of(table, table2))
                .relationships(List.of(relationship)));
    given(jobService.getActivePodCount()).willReturn(1);
  }

  @Test
  public void testTableValidations() throws Exception {
    DatasetSchemaUpdateModel updateModel =
        new DatasetSchemaUpdateModel()
            .description("test changeset")
            .changes(
                new DatasetSchemaUpdateModelChanges()
                    .addTables(
                        List.of(
                            new TableModel()
                                .name("new_table")
                                .columns(
                                    List.of(
                                        new ColumnModel()
                                            .name("new_table_column")
                                            .datatype(TableDataType.STRING))),
                            new TableModel()
                                .name(EXISTING_TABLE)
                                .columns(
                                    List.of(
                                        new ColumnModel()
                                            .name("new_column")
                                            .datatype(TableDataType.STRING))))));

    DatasetSchemaUpdateValidateModelStep validateModelStep =
        new DatasetSchemaUpdateValidateModelStep(datasetService, datasetId, updateModel);

    FlightContext flightContext = mock(FlightContext.class);

    StepResult stepResult = validateModelStep.doStep(flightContext);

    DatasetSchemaUpdateException exception =
        (DatasetSchemaUpdateException) stepResult.getException().orElseThrow();

    assertThat(exception.getMessage(), containsString("Could not validate"));
    assertThat(exception.getCauses().get(0), containsString("overwrite"));
    assertThat(exception.getCauses().get(1), is(EXISTING_TABLE));
    assertThat(exception.getCauses(), hasSize(2));
  }

  @Test
  public void testColumnDuplicatesValidations() throws Exception {
    DatasetSchemaUpdateModel duplicateColumnsUpdateModel =
        new DatasetSchemaUpdateModel()
            .description("test changeset")
            .changes(
                new DatasetSchemaUpdateModelChanges()
                    .addTables(
                        List.of(
                            new TableModel()
                                .name("new_table")
                                .columns(
                                    List.of(
                                        new ColumnModel()
                                            .name("new_table_column")
                                            .datatype(TableDataType.STRING)))))
                    .addColumns(
                        List.of(
                            new DatasetSchemaColumnUpdateModel()
                                .tableName(EXISTING_TABLE)
                                .columns(
                                    List.of(
                                        new ColumnModel()
                                            .name(EXISTING_COLUMN)
                                            .datatype(TableDataType.STRING),
                                        new ColumnModel()
                                            .name("new_column")
                                            .datatype(TableDataType.STRING))),
                            new DatasetSchemaColumnUpdateModel()
                                .tableName("new_table")
                                .columns(
                                    List.of(
                                        new ColumnModel()
                                            .name("new_table_column")
                                            .datatype(TableDataType.STRING))))));

    DatasetSchemaUpdateValidateModelStep validateModelStep =
        new DatasetSchemaUpdateValidateModelStep(
            datasetService, datasetId, duplicateColumnsUpdateModel);

    FlightContext flightContext = mock(FlightContext.class);

    DatasetSchemaUpdateException duplicateColumnsException =
        (DatasetSchemaUpdateException)
            validateModelStep.doStep(flightContext).getException().orElseThrow();

    assertThat(
        duplicateColumnsException.getMessage(),
        containsString("overwrite existing or to-be-added"));
    assertThat(duplicateColumnsException.getCauses(), hasSize(2));
    assertThat(
        duplicateColumnsException.getCauses(),
        equalTo(List.of("existing_table:existing_column", "new_table:new_table_column")));
  }

  @Test
  public void testColumnMissingTableValidations() throws Exception {
    DatasetSchemaUpdateModel missingTableUpdateModel =
        new DatasetSchemaUpdateModel()
            .changes(
                new DatasetSchemaUpdateModelChanges()
                    .addColumns(
                        List.of(
                            new DatasetSchemaColumnUpdateModel()
                                .tableName("not_a_real_table")
                                .columns(
                                    List.of(
                                        new ColumnModel()
                                            .name("new_column")
                                            .datatype(TableDataType.STRING))))));

    DatasetSchemaUpdateValidateModelStep validateModelStep =
        new DatasetSchemaUpdateValidateModelStep(
            datasetService, datasetId, missingTableUpdateModel);

    FlightContext flightContext = mock(FlightContext.class);

    DatasetSchemaUpdateException missingTableException =
        (DatasetSchemaUpdateException)
            validateModelStep.doStep(flightContext).getException().orElseThrow();

    assertThat(missingTableException.getMessage(), containsString("Could not find tables"));
    assertThat(missingTableException.getCauses(), contains(containsString("not_a_real_table")));
  }

  @Test
  public void testRelationshipDuplicateName() throws Exception {
    String newTableName = "new_table";
    String newColumnName = "new_column";
    RelationshipModel newRelationship =
        new RelationshipModel()
            .name(EXISTING_RELATIONSHIP)
            .from(new RelationshipTermModel().table(EXISTING_TABLE).column(EXISTING_COLUMN))
            .to(new RelationshipTermModel().table(newTableName).column(newColumnName));
    DatasetSchemaUpdateModel relationshipUpdateModel =
        new DatasetSchemaUpdateModel()
            .changes(
                new DatasetSchemaUpdateModelChanges().addRelationships(List.of(newRelationship)));

    DatasetSchemaUpdateValidateModelStep validateModelStep =
        new DatasetSchemaUpdateValidateModelStep(
            datasetService, datasetId, relationshipUpdateModel);

    FlightContext flightContext = mock(FlightContext.class);

    DatasetSchemaUpdateException invalidRelationshipException =
        (DatasetSchemaUpdateException)
            validateModelStep.doStep(flightContext).getException().orElseThrow();

    assertThat(
        invalidRelationshipException.getMessage(),
        containsString("Could not validate relationship additions"));
    assertTrue(
        invalidRelationshipException
            .getCauses()
            .get(0)
            .contains("Relationships with these names already exist for this dataset"));
    assertTrue(invalidRelationshipException.getCauses().get(1).contains(EXISTING_RELATIONSHIP));
  }

  @Test
  public void testRelationshipMissingTable() throws Exception {
    RelationshipModel newRelationship =
        new RelationshipModel()
            .name("new_relationship")
            .from(new RelationshipTermModel().table(EXISTING_TABLE).column(EXISTING_COLUMN))
            .to(
                new RelationshipTermModel()
                    .table("nonexistent_table")
                    .column("nonexistent_column"));
    DatasetSchemaUpdateModel relationshipUpdateModel =
        new DatasetSchemaUpdateModel()
            .changes(
                new DatasetSchemaUpdateModelChanges().addRelationships(List.of(newRelationship)));

    DatasetSchemaUpdateValidateModelStep validateModelStep =
        new DatasetSchemaUpdateValidateModelStep(
            datasetService, datasetId, relationshipUpdateModel);

    FlightContext flightContext = mock(FlightContext.class);

    DatasetSchemaUpdateException invalidRelationshipException =
        (DatasetSchemaUpdateException)
            validateModelStep.doStep(flightContext).getException().orElseThrow();

    assertThat(
        invalidRelationshipException.getMessage(),
        containsString("Could not validate relationship additions"));
    assertTrue(
        invalidRelationshipException
            .getCauses()
            .get(1)
            .contains("InvalidRelationshipTermTable: Invalid table nonexistent_table"));
  }

  @Test
  public void testRelationshipMissingColumn() throws Exception {
    String newTableName = "new_table";
    RelationshipModel newRelationship =
        new RelationshipModel()
            .name("relationship_missing_column")
            .from(new RelationshipTermModel().table(EXISTING_TABLE).column(EXISTING_COLUMN))
            .to(new RelationshipTermModel().table(newTableName).column("nonexistent_column"));
    DatasetSchemaUpdateModel relationshipUpdateModel =
        new DatasetSchemaUpdateModel()
            .changes(
                new DatasetSchemaUpdateModelChanges()
                    .addTables(
                        List.of(DatasetFixtures.tableModel(newTableName, List.of("new_column"))))
                    .addRelationships(List.of(newRelationship)));

    DatasetSchemaUpdateValidateModelStep validateModelStep =
        new DatasetSchemaUpdateValidateModelStep(
            datasetService, datasetId, relationshipUpdateModel);

    FlightContext flightContext = mock(FlightContext.class);

    DatasetSchemaUpdateException invalidRelationshipException =
        (DatasetSchemaUpdateException)
            validateModelStep.doStep(flightContext).getException().orElseThrow();

    assertThat(
        invalidRelationshipException.getMessage(),
        containsString("Could not validate relationship additions"));
    assertTrue(
        invalidRelationshipException
            .getCauses()
            .contains(
                "InvalidRelationshipTermTableColumn: invalid table new_table.nonexistent_column"));
  }

  @Test
  public void testColumnRelationshipInvalidType() throws Exception {
    String newTableName = "new_table";
    String newColumnName = "new_column";
    TableModel newTable =
        new TableModel()
            .name(newTableName)
            .columns(
                List.of(
                    DatasetFixtures.columnModel(
                        newColumnName, TableDataType.FILEREF, false, true)));
    RelationshipModel newRelationship =
        new RelationshipModel()
            .name("relationship_invalid_type")
            .from(new RelationshipTermModel().table(EXISTING_TABLE).column(EXISTING_COLUMN))
            .to(new RelationshipTermModel().table(newTableName).column(newColumnName));
    DatasetSchemaUpdateModel relationshipUpdateModel =
        new DatasetSchemaUpdateModel()
            .changes(
                new DatasetSchemaUpdateModelChanges()
                    .addTables(List.of(newTable))
                    .addRelationships(List.of(newRelationship)));

    DatasetSchemaUpdateValidateModelStep validateModelStep =
        new DatasetSchemaUpdateValidateModelStep(
            datasetService, datasetId, relationshipUpdateModel);

    FlightContext flightContext = mock(FlightContext.class);

    DatasetSchemaUpdateException invalidRelationshipException =
        (DatasetSchemaUpdateException)
            validateModelStep.doStep(flightContext).getException().orElseThrow();

    assertThat(
        invalidRelationshipException.getMessage(),
        containsString("Could not validate relationship additions"));
    assertThat(
        invalidRelationshipException.getCauses().get(1), containsString("InvalidForeignKey"));
    assertThat(
        invalidRelationshipException.getCauses().get(1),
        containsString(
            "InvalidForeignKey: Foreign key new_column cannot be a column with fileref type"));
  }
}
