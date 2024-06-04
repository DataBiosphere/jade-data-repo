package bio.terra.service.load;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.filedata.FSFileInfo;
import bio.terra.service.load.exception.LoadLockedException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Tag(Unit.TAG)
@EmbeddedDatabaseTest
class LoadDaoUnitTest {
  private final Logger logger = LoggerFactory.getLogger(LoadDaoUnitTest.class);

  @Autowired private LoadDao loadDao;

  @Autowired private ConfigurationService configService;

  private enum LoadTagsUsedByTest {
    LOADTAG_MY("myLoadTag"),
    LOADTAG_SERIAL("serialLoadTag"),
    LOADTAG_CONCURRENT("concurrentLoadTag"),
    LOADTAG_CONFLICT("conflictLoadTag");
    private final String tag;

    public String getTag() {
      return tag;
    }

    LoadTagsUsedByTest(String tag) {
      this.tag = tag;
    }
  }

  private enum FlightIdsUsedByTest {
    FLIGHT_MY("myFlightId"),
    FLIGHT_INIT("initFlightId"),
    FLIGHT_A("flightIdA"),
    FLIGHT_B("flightIdB"),
    FLIGHT_C("flightIdC"),
    FLIGHT_D("flightIdD"),
    FLIGHT_E("flightIdE"),
    FLIGHT_F("flightIdF"),
    FLIGHT_G("flightIdG"),
    FLIGHT_H("flightIdH"),
    FLIGHT_X("flightIdX"),
    FLIGHT_Y("flightIdY");
    private final String id;

    public String getId() {
      return id;
    }

    FlightIdsUsedByTest(String id) {
      this.id = id;
    }
  }

  private List<UUID> loadIdsWithFilesUsedByTest;

  /**
   * Any load tags and flight ids used in this class should be added to the enums above. Before each
   * test method is run, we try to unlock each combination of load tag + flight id. This is to
   * prevent leftover state from impacting the test results, so that the tests are repeatable. The
   * loop below is exhaustive in trying to unlock every combination, but this allows each test
   * method to not worry about lock cleanup, and not worry about interactions with other test
   * methods.
   */
  @BeforeEach
  void setup() {
    // try to unlock all load tags in the enum
    for (LoadTagsUsedByTest loadTag : LoadTagsUsedByTest.values()) {
      // loop through all flight ids in the enum, since any one could have successfully locked the
      // load last
      for (FlightIdsUsedByTest flightId : FlightIdsUsedByTest.values()) {
        try {
          loadDao.unlockLoad(loadTag.getTag(), flightId.getId());
        } catch (RuntimeException rEx) {
        }
      }
    }

    // initialize the load id list to an empty list
    loadIdsWithFilesUsedByTest = new ArrayList<>();
  }

  /**
   * Any load ids that add files should be added to the list above. After each test method is run,
   * we try to clear all the files for each load id in the list. This is to prevent leftover state
   * from impacting test results, so that the tests are repeatable.
   */
  @AfterEach
  void teardown() {
    // try to clean files for all load ids in the list
    for (UUID loadId : loadIdsWithFilesUsedByTest) {
      try {
        loadDao.cleanFiles(loadId);
      } catch (RuntimeException rEx) {
        logger.error("Error cleaning files for load id: " + loadId, rEx);
      }
    }
  }

  @Test
  void loadFilesTest() throws Exception {
    UUID loadId = populateFiles(8);

    // First set of candidates
    LoadCandidates candidates = loadDao.findCandidates(loadId, 3);
    testLoadCandidates(candidates, 0, 0, 3);

    List<LoadFile> loadSet1 = candidates.getCandidateFiles();
    FSFileInfo fsFileInfo;
    fsFileInfo = new FSFileInfo().checksumCrc32c("crcChecksum").checksumMd5("md5Checksum");

    loadDao.setLoadFileSucceeded(loadId, loadSet1.get(0).getTargetPath(), "fileidA", fsFileInfo);
    loadDao.setLoadFileFailed(loadId, loadSet1.get(1).getTargetPath(), "failureB");
    loadDao.setLoadFileRunning(
        loadId, loadSet1.get(2).getTargetPath(), FlightIdsUsedByTest.FLIGHT_C.getId());

    // Second set of candidates - set prior running to succeeded
    candidates = loadDao.findCandidates(loadId, 3);
    testLoadCandidates(candidates, 1, 1, 3);
    List<LoadFile> loadSet2 = candidates.getCandidateFiles();

    loadDao.setLoadFileSucceeded(loadId, loadSet1.get(2).getTargetPath(), "fileidC", fsFileInfo);

    loadDao.setLoadFileRunning(
        loadId, loadSet2.get(0).getTargetPath(), FlightIdsUsedByTest.FLIGHT_D.getId());
    loadDao.setLoadFileRunning(
        loadId, loadSet2.get(1).getTargetPath(), FlightIdsUsedByTest.FLIGHT_E.getId());
    loadDao.setLoadFileRunning(
        loadId, loadSet2.get(2).getTargetPath(), FlightIdsUsedByTest.FLIGHT_F.getId());

    // Third set of candidates - set all 3 prior to failed
    candidates = loadDao.findCandidates(loadId, 3);
    testLoadCandidates(candidates, 1, 3, 2);
    List<LoadFile> loadSet3 = candidates.getCandidateFiles();

    loadDao.setLoadFileFailed(loadId, loadSet2.get(0).getTargetPath(), "errorD");
    loadDao.setLoadFileFailed(loadId, loadSet2.get(1).getTargetPath(), "errorE");
    loadDao.setLoadFileFailed(loadId, loadSet2.get(2).getTargetPath(), "errorF");

    loadDao.setLoadFileRunning(
        loadId, loadSet3.get(0).getTargetPath(), FlightIdsUsedByTest.FLIGHT_G.getId());
    loadDao.setLoadFileRunning(
        loadId, loadSet3.get(1).getTargetPath(), FlightIdsUsedByTest.FLIGHT_H.getId());

    // No more candidates, but things are still running
    candidates = loadDao.findCandidates(loadId, 3);
    testLoadCandidates(candidates, 4, 2, 0);

    loadDao.setLoadFileSucceeded(loadId, loadSet3.get(0).getTargetPath(), "fileidG", fsFileInfo);
    loadDao.setLoadFileSucceeded(loadId, loadSet3.get(1).getTargetPath(), "fileidH", fsFileInfo);

    // No more candidates and nothing running; this would be the bulk load completed state
    candidates = loadDao.findCandidates(loadId, 3);
    testLoadCandidates(candidates, 4, 0, 0);

    // clean up after ourselves - check that we properly find nothing
    loadDao.cleanFiles(loadId);
    candidates = loadDao.findCandidates(loadId, 3);
    testLoadCandidates(candidates, 0, 0, 0);
  }

  @Test
  void serialLockTest() throws Exception {
    final String loadTag = LoadTagsUsedByTest.LOADTAG_SERIAL.getTag();
    final String flightX = FlightIdsUsedByTest.FLIGHT_X.getId();
    final String flightY = FlightIdsUsedByTest.FLIGHT_Y.getId();

    boolean xlocks = tryLockLoad(loadTag, flightX);
    assertThat("x gets lock", xlocks);

    xlocks = tryLockLoad(loadTag, flightX);
    assertThat("x gets lock again", xlocks);

    boolean ylocks = tryLockLoad(loadTag, flightY);
    assertFalse(ylocks, "y does not get lock");

    loadDao.unlockLoad(loadTag, flightX);

    ylocks = tryLockLoad(loadTag, flightY);
    assertThat("y gets lock", ylocks);

    // No errors unlocking X again
    loadDao.unlockLoad(loadTag, flightX);

    loadDao.unlockLoad(loadTag, flightY);
  }

  private boolean tryLockLoad(String loadTag, String flightId) throws InterruptedException {
    try {
      loadDao.lockLoad(loadTag, flightId);
      return true;
    } catch (LoadLockedException ex) {
      return false;
    }
  }

  @Test
  void conflictLockTest() throws Exception {
    final String loadTag = LoadTagsUsedByTest.LOADTAG_CONFLICT.getTag();

    loadDao.lockLoad(loadTag, FlightIdsUsedByTest.FLIGHT_INIT.getId());
    loadDao.unlockLoad(loadTag, FlightIdsUsedByTest.FLIGHT_INIT.getId());

    LoadLockUnlockLooper looperA =
        new LoadLockUnlockLooper(loadDao, loadTag, FlightIdsUsedByTest.FLIGHT_A.getId(), 100);
    LoadLockUnlockLooper looperB =
        new LoadLockUnlockLooper(loadDao, loadTag, FlightIdsUsedByTest.FLIGHT_B.getId(), 100);

    Thread threadA = new Thread(looperA);
    Thread threadB = new Thread(looperB);

    threadA.start();
    threadB.start();
    threadA.join();
    threadB.join();

    assertThat(looperA.getConflicts(), is(0));
    assertThat(looperB.getConflicts(), is(0));
  }

  private void testLoadCandidates(
      LoadCandidates candidates, int failures, int running, int notTried) {
    assertThat("right number of failures", candidates.getFailedLoads(), equalTo(failures));
    assertThat("right number of running", candidates.getRunningLoads().size(), equalTo(running));
    assertThat(
        "right number of not_tried", candidates.getCandidateFiles().size(), equalTo(notTried));
  }

  private UUID populateFiles(int n) throws InterruptedException {
    Load load =
        loadDao.lockLoad(
            LoadTagsUsedByTest.LOADTAG_MY.getTag(), FlightIdsUsedByTest.FLIGHT_MY.getId());
    // add load id to test class list, for cleanup afterward
    loadIdsWithFilesUsedByTest.add(load.getId());

    List<BulkLoadFileModel> loadList =
        IntStream.range(0, n)
            .mapToObj(
                i ->
                    new BulkLoadFileModel()
                        .sourcePath("gs://path" + i)
                        .targetPath("/target/path" + i)
                        .description("number " + i))
            .toList();

    loadDao.populateFiles(load.getId(), loadList);
    return load.getId();
  }
}
