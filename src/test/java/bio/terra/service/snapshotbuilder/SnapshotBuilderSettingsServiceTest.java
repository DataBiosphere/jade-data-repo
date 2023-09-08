package bio.terra.service.snapshotbuilder;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import bio.terra.common.category.Unit;
import bio.terra.model.SnapshotBuilderDatasetConceptSets;
import bio.terra.model.SnapshotBuilderSettings;
import java.util.List;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@RunWith(MockitoJUnitRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class SnapshotBuilderSettingsServiceTest {
  @Mock private SnapshotBuilderSettingsDao snapshotBuilderSettingsDao;

  private SnapshotBuilderService snapshotBuilderService;

  @Before
  public void setUp() {
    snapshotBuilderService = new SnapshotBuilderService(snapshotBuilderSettingsDao);
  }

  @Test
  public void testGetSnapshotBuilderSettings() {
    UUID datasetId = UUID.randomUUID();
    snapshotBuilderService.getSnapshotBuilderSettings(datasetId);
    verify(snapshotBuilderSettingsDao, times(1)).getSnapshotBuilderSettingsByDatasetId(datasetId);
  }

  @Test
  public void testUpdateSnapshotBuilderSettings() {
    UUID datasetId = UUID.randomUUID();
    SnapshotBuilderSettings settings =
        new SnapshotBuilderSettings()
            .datasetConceptSets(List.of(new SnapshotBuilderDatasetConceptSets()));
    snapshotBuilderService.updateSnapshotBuilderSettings(datasetId, settings);
    verify(snapshotBuilderSettingsDao, times(1))
        .upsertSnapshotBuilderSettingsByDataset(datasetId, settings);
  }
}
