package bio.terra.service.load;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.DaoOperations;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FSFileInfo;
import bio.terra.service.load.exception.LoadLockedException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Tag(Unit.TAG)
@EmbeddedDatabaseTest
class LoadDaoUnitTest {

  @Autowired private DaoOperations daoOperations;
  @Autowired private LoadDao loadDao;

  private Dataset dataset;

  @BeforeEach
  void beforeEach() throws IOException {
    dataset = daoOperations.createDataset();
  }

  @Test
  void loadFilesTest() {
    LoadLockKey loadLockKey = new LoadLockKey("myLoadTag", dataset.getId());
    UUID loadId = loadDao.lockLoad(loadLockKey, "myFlightId").id();
    List<BulkLoadFileModel> loadList = new ArrayList<>();
    for (int i = 0; i < 8; i++) {
      loadList.add(
          new BulkLoadFileModel()
              .sourcePath("gs://path" + i)
              .targetPath("/target/path" + i)
              .description("number " + i));
    }
    loadDao.populateFiles(loadId, loadList);

    // First set of candidates
    LoadCandidates candidates = loadDao.findCandidates(loadId, 3);
    testLoadCandidates(candidates, 0, 0, 3);

    List<LoadFile> loadSet1 = candidates.getCandidateFiles();
    FSFileInfo fsFileInfo;
    fsFileInfo = new FSFileInfo().checksumCrc32c("crcChecksum").checksumMd5("md5Checksum");

    loadDao.setLoadFileSucceeded(loadId, loadSet1.get(0).getTargetPath(), "fileidA", fsFileInfo);
    loadDao.setLoadFileFailed(loadId, loadSet1.get(1).getTargetPath(), "failureB");
    loadDao.setLoadFileRunning(loadId, loadSet1.get(2).getTargetPath(), "flightIdC");

    // Second set of candidates - set prior running to succeeded
    candidates = loadDao.findCandidates(loadId, 3);
    testLoadCandidates(candidates, 1, 1, 3);
    List<LoadFile> loadSet2 = candidates.getCandidateFiles();

    loadDao.setLoadFileSucceeded(loadId, loadSet1.get(2).getTargetPath(), "fileidC", fsFileInfo);

    loadDao.setLoadFileRunning(loadId, loadSet2.get(0).getTargetPath(), "flightIdD");
    loadDao.setLoadFileRunning(loadId, loadSet2.get(1).getTargetPath(), "flightIdE");
    loadDao.setLoadFileRunning(loadId, loadSet2.get(2).getTargetPath(), "flightIdF");

    // Third set of candidates - set all 3 prior to failed
    candidates = loadDao.findCandidates(loadId, 3);
    testLoadCandidates(candidates, 1, 3, 2);
    List<LoadFile> loadSet3 = candidates.getCandidateFiles();

    loadDao.setLoadFileFailed(loadId, loadSet2.get(0).getTargetPath(), "errorD");
    loadDao.setLoadFileFailed(loadId, loadSet2.get(1).getTargetPath(), "errorE");
    loadDao.setLoadFileFailed(loadId, loadSet2.get(2).getTargetPath(), "errorF");

    loadDao.setLoadFileRunning(loadId, loadSet3.get(0).getTargetPath(), "flightIdG");
    loadDao.setLoadFileRunning(loadId, loadSet3.get(1).getTargetPath(), "flightIdH");

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
  void serialLockTest() throws IOException {
    final String loadTag = "serialLoadTag";
    final String flightX = "flightIdX";
    final String flightY = "flightIdY";
    final String flightZ = "flightIdZ";
    final UUID datasetIdA = dataset.getId();
    final LoadLockKey loadTag_datasetA = new LoadLockKey(loadTag, datasetIdA);

    assertThat(
        "flightX gets lock on " + loadTag_datasetA,
        loadDao.lockLoad(loadTag_datasetA, flightX).lockingFlightId(),
        equalTo(flightX));
    assertThat(
        "flightX gets lock on " + loadTag_datasetA + " again",
        loadDao.lockLoad(loadTag_datasetA, flightX).lockingFlightId(),
        equalTo(flightX));
    assertThrows(
        LoadLockedException.class,
        () -> loadDao.lockLoad(loadTag_datasetA, flightY),
        "flightY does not get lock while flightX locks " + loadTag_datasetA);

    loadDao.unlockLoad(loadTag_datasetA, flightX);
    assertThat(
        "flightY gets lock once flightX unlocks " + loadTag_datasetA,
        loadDao.lockLoad(loadTag_datasetA, flightY).lockingFlightId(),
        equalTo(flightY));

    final UUID datasetIdB = daoOperations.createDataset().getId();
    final LoadLockKey loadTag_datasetB = new LoadLockKey(loadTag, datasetIdB);
    assertThat(
        "flightZ gets lock on "
            + loadTag_datasetB
            + " even with flightY's lock on "
            + loadTag_datasetA,
        loadDao.lockLoad(loadTag_datasetB, flightZ).lockingFlightId(),
        equalTo(flightZ));

    // No errors unlocking X again
    loadDao.unlockLoad(loadTag_datasetA, flightX);

    loadDao.unlockLoad(loadTag_datasetA, flightY);
    loadDao.unlockLoad(loadTag_datasetB, flightZ);
  }

  private void testLoadCandidates(
      LoadCandidates candidates, int failures, int running, int notTried) {
    assertThat("right number of failures", candidates.getFailedLoads(), equalTo(failures));
    assertThat("right number of running", candidates.getRunningLoads(), hasSize(running));
    assertThat("right number of not_tried", candidates.getCandidateFiles(), hasSize(notTried));
  }
}
