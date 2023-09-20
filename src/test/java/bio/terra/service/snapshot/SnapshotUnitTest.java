package bio.terra.service.snapshot;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import bio.terra.common.category.Unit;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = {"datarepo.testWithEmbeddedDatabase=false"})
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class SnapshotUnitTest {
  @Test
  public void isSnapshot() {
    Snapshot snapshot = new Snapshot();
    assertTrue(snapshot.isSnapshot());
  }

  @Test
  public void isDataset() {
    Snapshot snapshot = new Snapshot();
    assertFalse(snapshot.isDataset());
  }
}
