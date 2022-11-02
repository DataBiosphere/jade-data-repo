package bio.terra.service.snapshot.flight.duos;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import java.util.UUID;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class SnapshotDuosFlightUtilsTest {

  @Mock FlightContext context;

  @Test
  public void testGetFirecloudGroup() {
    FlightMap workingMap = new FlightMap();
    when(context.getWorkingMap()).thenReturn(workingMap);

    assertNull(SnapshotDuosFlightUtils.getFirecloudGroup(context));

    DuosFirecloudGroupModel group = new DuosFirecloudGroupModel().id(UUID.randomUUID());
    workingMap.put(SnapshotDuosMapKeys.FIRECLOUD_GROUP, group);
    assertThat(SnapshotDuosFlightUtils.getFirecloudGroup(context), equalTo(group));
  }

  @Test
  public void testGetDuosFirecloudGroupId() {
    assertNull(SnapshotDuosFlightUtils.getDuosFirecloudGroupId(null));
    assertNull(SnapshotDuosFlightUtils.getDuosFirecloudGroupId(new DuosFirecloudGroupModel()));

    UUID id = UUID.randomUUID();
    DuosFirecloudGroupModel groupWithId = new DuosFirecloudGroupModel().id(id);
    assertThat(SnapshotDuosFlightUtils.getDuosFirecloudGroupId(groupWithId), equalTo(id));
  }
}
