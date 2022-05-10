package bio.terra.service.dataset.flight.update;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import bio.terra.common.Column;
import bio.terra.common.category.Unit;
import bio.terra.model.ColumnModel;
import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.model.DatasetSchemaUpdateModelChanges;
import bio.terra.model.TableDataType;
import bio.terra.model.TableModel;
import bio.terra.service.configuration.ConfigurationService;
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
  private static final String EXISTING_TABLE_NAME = "existing_table";

  @Before
  public void setup() {
    datasetId = UUID.randomUUID();
    UUID existingTableId = UUID.randomUUID();
    UUID existingColumnId = UUID.randomUUID();
    when(datasetService.retrieve(datasetId))
        .thenReturn(
            new Dataset()
                .id(datasetId)
                .name("ValidationTestDataset")
                .description("A dataset to test schema update validation")
                .tables(
                    List.of(
                        new DatasetTable()
                            .name(EXISTING_TABLE_NAME)
                            .id(existingTableId)
                            .columns(
                                List.of(
                                    new Column()
                                        .name("existing_column")
                                        .id(existingColumnId)
                                        .type(TableDataType.STRING)
                                        .arrayOf(false)
                                        .required(false))))));
    given(jobService.getActivePodCount()).willReturn(1);
  }

  @Test
  public void testValidations() throws Exception {
    DatasetSchemaUpdateModel updateModel =
        new DatasetSchemaUpdateModel()
            .description("test changeset")
            .changes(
                new DatasetSchemaUpdateModelChanges()
                    .addTables(
                        List.of(
                            new TableModel()
                                .name(EXISTING_TABLE_NAME)
                                .columns(
                                    List.of(
                                        new ColumnModel()
                                            .name("new_column")
                                            .datatype(TableDataType.STRING)
                                            .arrayOf(false)
                                            .required(false))))));

    DatasetSchemaUpdateValidateModelStep validateModelStep =
        new DatasetSchemaUpdateValidateModelStep(datasetService, datasetId, updateModel);

    FlightContext flightContext = mock(FlightContext.class);

    StepResult stepResult = validateModelStep.doStep(flightContext);

    DatasetSchemaUpdateException exception =
        (DatasetSchemaUpdateException) stepResult.getException().orElseThrow();

    assertThat(exception.getMessage(), containsString("Could not validate"));
    assertThat(exception.getCauses().get(0), containsString("overwrite"));
    assertThat(exception.getCauses().get(1), is(EXISTING_TABLE_NAME));
  }
}
