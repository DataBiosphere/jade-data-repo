package bio.terra.service.load;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

import bio.terra.common.category.Unit;
import bio.terra.service.load.exception.LoadLockFailureException;
import bio.terra.service.load.exception.LoadLockedException;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class LoadUnitTest {
  @Autowired private LoadService loadService;

  private enum LoadTagsUsedByTest {
    LOADTAG_1("myLoadTag1"),
    LOADTAG_2("myLoadTag2");
    private String tag;

    public String getTag() {
      return tag;
    }

    LoadTagsUsedByTest(String tag) {
      this.tag = tag;
    }
  }

  private enum FlightIdsUsedByTest {
    FLIGHT_1("myFlightId1"),
    FLIGHT_2("myFlightId2");
    private String id;

    public String getId() {
      return id;
    }

    FlightIdsUsedByTest(String id) {
      this.id = id;
    }
  }

  /**
   * Any load tags and flight ids used in this class should be added to the enums above. Before each
   * test method is run, we try to unlock each combination of load tag + flight id. This is to
   * prevent leftover state from impacting the test results, so that the tests are repeatable. The
   * loop below is exhaustive in trying to unlock every combination, but this allows each test
   * method to not worry about lock cleanup, and not worry about interactions with other test
   * methods.
   */
  @Before
  public void setup() throws Exception {
    // try to unlock all load tags in the enum
    for (LoadTagsUsedByTest loadTag : LoadTagsUsedByTest.values()) {
      // loop through all flight ids in the enum, since any one could have successfully locked the
      // load last
      for (FlightIdsUsedByTest flightId : FlightIdsUsedByTest.values()) {
        try {
          loadService.unlockLoad(loadTag.getTag(), flightId.getId());
        } catch (RuntimeException rEx) {
        }
      }
    }
  }

  @Test
  public void loadLocKTest() throws Exception {
    loadService.lockLoad(
        LoadTagsUsedByTest.LOADTAG_1.getTag(), FlightIdsUsedByTest.FLIGHT_1.getId());
    // Relock of the same (tag, flight) should work
    loadService.lockLoad(
        LoadTagsUsedByTest.LOADTAG_1.getTag(), FlightIdsUsedByTest.FLIGHT_1.getId());
    loadService.lockLoad(
        LoadTagsUsedByTest.LOADTAG_2.getTag(), FlightIdsUsedByTest.FLIGHT_2.getId());
    loadService.unlockLoad(
        LoadTagsUsedByTest.LOADTAG_2.getTag(), FlightIdsUsedByTest.FLIGHT_2.getId());
    loadService.unlockLoad(
        LoadTagsUsedByTest.LOADTAG_1.getTag(), FlightIdsUsedByTest.FLIGHT_1.getId());
    // Duplicate unlock should work
    loadService.unlockLoad(
        LoadTagsUsedByTest.LOADTAG_1.getTag(), FlightIdsUsedByTest.FLIGHT_1.getId());
  }

  @Test(expected = LoadLockedException.class)
  public void alreadyLockedTest() throws Exception {
    loadService.lockLoad(
        LoadTagsUsedByTest.LOADTAG_1.getTag(), FlightIdsUsedByTest.FLIGHT_1.getId());
    loadService.lockLoad(
        LoadTagsUsedByTest.LOADTAG_1.getTag(), FlightIdsUsedByTest.FLIGHT_2.getId());
  }

  @Test
  public void cannotUnlockTest() throws Exception {
    // Unlock with the wrong flight succeeds. That is because there is a valid case:
    // flight2 had the lock and did an unlock, but failed before the step completed.
    // Flight1 gets the lock. Flight2 recovers and re-runs the unlock.
    loadService.lockLoad(
        LoadTagsUsedByTest.LOADTAG_1.getTag(), FlightIdsUsedByTest.FLIGHT_1.getId());
    loadService.unlockLoad(
        LoadTagsUsedByTest.LOADTAG_1.getTag(), FlightIdsUsedByTest.FLIGHT_2.getId());
  }

  @Test
  public void computeLoadTagTest() throws Exception {
    String loadTag = loadService.computeLoadTag(null);
    assertThat("generated load tag", loadTag, startsWith("load-at-"));
    loadTag = loadService.computeLoadTag(LoadTagsUsedByTest.LOADTAG_1.getTag());
    assertThat("pass through load tag", loadTag, equalTo(LoadTagsUsedByTest.LOADTAG_1.getTag()));
  }

  @Test
  public void getLoadTagTest() throws Exception {
    // Should get tag from working map
    FlightMap inputParams = new FlightMap();
    FlightContext flightContext = new FlightContext(inputParams, null, Collections.emptyList());
    FlightMap workingMap = flightContext.getWorkingMap();
    workingMap.put(LoadMapKeys.LOAD_TAG, LoadTagsUsedByTest.LOADTAG_1.getTag());

    String loadTag = loadService.getLoadTag(flightContext);
    assertThat("working map load tag", loadTag, equalTo(LoadTagsUsedByTest.LOADTAG_1.getTag()));

    // Should get from input Params
    FlightMap inputParams1 = new FlightMap();
    inputParams1.put(LoadMapKeys.LOAD_TAG, LoadTagsUsedByTest.LOADTAG_1.getTag());
    flightContext = new FlightContext(inputParams1, null, Collections.emptyList());
    workingMap = flightContext.getWorkingMap();
    workingMap.put(LoadMapKeys.LOAD_TAG, LoadTagsUsedByTest.LOADTAG_2.getTag());

    loadTag = loadService.getLoadTag(flightContext);
    assertThat("input params load tag", loadTag, equalTo(LoadTagsUsedByTest.LOADTAG_1.getTag()));
  }

  @Test(expected = LoadLockFailureException.class)
  public void getLoadTagFailTest() throws Exception {
    FlightMap inputParams = new FlightMap();
    FlightContext flightContext = new FlightContext(inputParams, null, Collections.emptyList());
    loadService.getLoadTag(flightContext);
  }
}
