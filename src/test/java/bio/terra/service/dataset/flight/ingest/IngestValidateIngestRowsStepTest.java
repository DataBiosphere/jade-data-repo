package bio.terra.service.dataset.flight.ingest;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

import bio.terra.common.Column;
import bio.terra.common.category.Unit;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.exception.InvalidIngestDuplicatesException;
import bio.terra.service.tabulardata.google.bigquery.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class IngestValidateIngestRowsStepTest {

  @Mock private DatasetService datasetService;
  @Mock private FlightContext flightContext;
  @Mock private TableResult tableResult;

  private MockedStatic<IngestUtils> mockedUtils;
  private MockedStatic<BigQueryPdao> mockedBigQueryPdao;

  private IngestValidateIngestRowsStep step;

  @BeforeEach
  void setUp() {
    mockedUtils = mockStatic(IngestUtils.class);
    mockedBigQueryPdao = mockStatic(BigQueryPdao.class);
  }

  @AfterEach
  void close() {
    mockedUtils.close();
    mockedBigQueryPdao.close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testDoStep(boolean postIngest) throws InterruptedException {
    step = new IngestValidateIngestRowsStep(datasetService, postIngest);
    Dataset dataset = createDataset();
    DatasetTable datasetTable = dataset.getTables().get(0);

    when(IngestUtils.getDataset(flightContext, datasetService)).thenReturn(dataset);
    when(IngestUtils.getDatasetTable(flightContext, dataset)).thenReturn(datasetTable);
    String tableName;
    if (postIngest) {
      tableName = datasetTable.getName();
    } else {
      tableName = "staging_table";
      when(IngestUtils.getStagingTableName(flightContext)).thenReturn(tableName);
    }
    when(IngestUtils.getStagingTableName(flightContext)).thenReturn(tableName);

    // Mock behavior for no duplicates
    when(BigQueryPdao.duplicatePrimaryKeys(dataset, datasetTable.getPrimaryKey(), tableName))
        .thenReturn(tableResult);
    when(tableResult.getTotalRows()).thenReturn(0L);

    StepResult result = step.doStep(flightContext);
    assertTrue(result.isSuccess());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testDoStepWithDuplicatePrimaryKeys(boolean postIngest) throws InterruptedException {
    step = new IngestValidateIngestRowsStep(datasetService, postIngest);
    Dataset dataset = createDataset();
    DatasetTable datasetTable = dataset.getTables().get(0);

    when(IngestUtils.getDataset(flightContext, datasetService)).thenReturn(dataset);
    when(IngestUtils.getDatasetTable(flightContext, dataset)).thenReturn(datasetTable);
    String tableName;
    if (postIngest) {
      tableName = datasetTable.getName();
    } else {
      tableName = "staging_table";
      when(IngestUtils.getStagingTableName(flightContext)).thenReturn(tableName);
    }

    // Mocking of FieldValueList and FieldValue required for error details/causes
    FieldValueList row = mock(FieldValueList.class);
    FieldValue idValue = mock(FieldValue.class);
    FieldValue countValue = mock(FieldValue.class);
    when(row.get("id")).thenReturn(idValue);
    when(row.hasSchema()).thenReturn(true);
    when(row.get("count")).thenReturn(countValue);
    when(idValue.getStringValue()).thenReturn("123");
    when(countValue.getStringValue()).thenReturn("3");
    when(tableResult.getTotalRows()).thenReturn(5L);
    when(tableResult.iterateAll()).thenReturn(List.of(row));
    when(BigQueryPdao.duplicatePrimaryKeys(dataset, datasetTable.getPrimaryKey(), tableName))
        .thenReturn(tableResult);

    InvalidIngestDuplicatesException exception =
        assertThrows(InvalidIngestDuplicatesException.class, () -> step.doStep(flightContext));

    // Verify exception message
    assertTrue(exception.getMessage().contains("Duplicate primary keys identified"));
    assertTrue(exception.getCauses().contains("3 ingest rows with id=123"));
  }

  private Dataset createDataset() {
    Dataset dataset = new Dataset();

    DatasetTable datasetTable = new DatasetTable();
    Column primaryKeyColumn = new Column();
    primaryKeyColumn.name("id");
    datasetTable.primaryKey(List.of(primaryKeyColumn));
    dataset.tables(List.of(datasetTable));
    return dataset;
  }
}
