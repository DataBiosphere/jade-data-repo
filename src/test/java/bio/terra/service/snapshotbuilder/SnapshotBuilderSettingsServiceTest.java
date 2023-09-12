package bio.terra.service.snapshotbuilder;

import static org.mockito.Mockito.verify;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Tag(Unit.TAG)
@EmbeddedDatabaseTest
public class SnapshotBuilderSettingsServiceTest {
  @Mock private SnapshotBuilderSettingsDao snapshotBuilderSettingsDao;

  private SnapshotBuilderService snapshotBuilderService;

  @BeforeEach
  public void setUp() {
    snapshotBuilderService = new SnapshotBuilderService(snapshotBuilderSettingsDao);
  }

  @Test
  public void testGetSnapshotBuilderSettings() {
    UUID datasetId = UUID.randomUUID();
    snapshotBuilderService.getSnapshotBuilderSettings(datasetId);
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
