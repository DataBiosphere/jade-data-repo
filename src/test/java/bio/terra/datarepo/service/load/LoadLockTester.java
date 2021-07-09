package bio.terra.datarepo.service.load;

import bio.terra.datarepo.service.load.exception.LoadLockedException;

public class LoadLockTester implements Runnable {
  private LoadDao loadDao;
  private String loadTag;
  private String flightId;
  private int result;

  public LoadLockTester(LoadDao loadDao, String loadTag, String flightId) {
    this.loadDao = loadDao;
    this.loadTag = loadTag;
    this.flightId = flightId;
    this.result = 0;
  }

  @Override
  public void run() {
    try {
      loadDao.lockLoad(loadTag, flightId);
      result = 1;
    } catch (LoadLockedException ex) {
      result = 2;
    } catch (InterruptedException ex) {
      result = 3;
    }
  }

  public int getResult() {
    return result;
  }
}
