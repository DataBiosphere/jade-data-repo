package bio.terra.service.load;

import static bio.terra.service.load.LoadIsLockedBy.loadIsLockedBy;
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
    UUID loadId = loadDao.lockLoad("myLoadTag", "myFlightId", dataset.getId()).id();
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

    assertThat(
        "[Flight=X,dataset=A] gets lock",
        loadDao.lockLoad(loadTag, flightX, datasetIdA),
        loadIsLockedBy(flightX, datasetIdA));
    assertThat(
        "[Flight=X,dataset=A] gets lock again",
        loadDao.lockLoad(loadTag, flightX, datasetIdA),
        loadIsLockedBy(flightX, datasetIdA));
    assertThrows(
        LoadLockedException.class,
        () -> loadDao.lockLoad(loadTag, flightY, datasetIdA),
        "[Flight=Y,dataset=A] does not get lock");

    loadDao.unlockLoad(loadTag, flightX, datasetIdA);
    assertThat(
        "[Flight=Y,dataset=A] gets lock once [Flight=X,dataset=A] unlocks",
        loadDao.lockLoad(loadTag, flightY, datasetIdA),
        loadIsLockedBy(flightY, datasetIdA));

    final UUID datasetIdB = daoOperations.createDataset().getId();
    assertThat(
        "[Flight=Z,dataset=B] gets lock even with [Flight=Y,dataset=A] lock",
        loadDao.lockLoad(loadTag, flightZ, datasetIdB),
        loadIsLockedBy(flightZ, datasetIdB));

    // No errors unlocking X again
    loadDao.unlockLoad(loadTag, flightX, datasetIdA);

    loadDao.unlockLoad(loadTag, flightY, datasetIdA);
    loadDao.unlockLoad(loadTag, flightZ, datasetIdB);
  }

  private void testLoadCandidates(
      LoadCandidates candidates, int failures, int running, int notTried) {
    assertThat("right number of failures", candidates.getFailedLoads(), equalTo(failures));
    assertThat("right number of running", candidates.getRunningLoads(), hasSize(running));
    assertThat("right number of not_tried", candidates.getCandidateFiles(), hasSize(notTried));
  }
}
