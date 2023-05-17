package bio.terra.service.dataset;

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
public class DatasetUnitTest {
  @Test
  public void isSnapshot() {
    Dataset dataset = new Dataset();
    assertFalse(dataset.isSnapshot());
  }

  @Test
  public void isDataset() {
    Dataset dataset = new Dataset();
    assertTrue(dataset.isDataset());
  }
}
