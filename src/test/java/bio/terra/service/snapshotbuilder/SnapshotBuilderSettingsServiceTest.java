package bio.terra.service.snapshotbuilder;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.ColumnStatisticsIntModel;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.AzureSynapseService;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@Tag(Unit.TAG)
@ExtendWith(MockitoExtension.class)
public class SnapshotBuilderSettingsServiceTest {
  private AuthenticatedUserRequest testUser = AuthenticationFixtures.randomUserRequest();

  @Mock private SnapshotBuilderSettingsDao snapshotBuilderSettingsDao;
  @Mock private AzureSynapsePdao azureSynapsePdao;
  @Mock private AzureSynapseService azureSynapseService;
  @Mock private DatasetDao datasetDao;

  private SnapshotBuilderService snapshotBuilderService;

  @BeforeEach
  public void setUp() {
    snapshotBuilderService =
        new SnapshotBuilderService(
            snapshotBuilderSettingsDao, azureSynapsePdao, azureSynapseService, datasetDao);
  }

  @Test
  public void testGetSnapshotBuilderSettings() {
    UUID datasetId = UUID.randomUUID();
    when(snapshotBuilderSettingsDao.getSnapshotBuilderSettingsByDatasetId(datasetId))
        .thenReturn(SnapshotBuilderTestData.SETTINGS);
    when(azureSynapsePdao.getStatsForIntColumn(any(), any(), any(), any()))
        .thenReturn(new ColumnStatisticsIntModel().minValue(0).maxValue(10));
    when(datasetDao.retrieve(datasetId)).thenReturn(SnapshotBuilderTestData.DATASET);
    snapshotBuilderService.getSnapshotBuilderSettings(datasetId, testUser);
    verify(snapshotBuilderSettingsDao).getSnapshotBuilderSettingsByDatasetId(datasetId);
  }

  @Test
  public void testUpdateSnapshotBuilderSettings() {
    UUID datasetId = UUID.randomUUID();
    snapshotBuilderService.updateSnapshotBuilderSettings(
        datasetId, SnapshotBuilderTestData.SETTINGS);
    verify(snapshotBuilderSettingsDao)
        .upsertSnapshotBuilderSettingsByDataset(datasetId, SnapshotBuilderTestData.SETTINGS);
  }
}
