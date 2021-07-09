package bio.terra.datarepo.service.load;

import bio.terra.datarepo.service.load.exception.LoadLockedException;

public class LoadLockUnlockLooper implements Runnable {
  private LoadDao loadDao;
  private String loadTag;
  private String flightId;
  private int count;
  private int conflicts;

  public LoadLockUnlockLooper(LoadDao loadDao, String loadTag, String flightId, int count) {
    this.loadDao = loadDao;
    this.loadTag = loadTag;
    this.flightId = flightId;
    this.count = count;
  }

  @Override
  public void run() {
    conflicts = 0;
    for (int i = 0; i < count; ) {
      try {
        loadDao.lockLoad(loadTag, flightId);
        loadDao.unlockLoad(loadTag, flightId);
        i++;
      } catch (LoadLockedException ex) {
        conflicts++;
      } catch (InterruptedException ex) {
        return;
      }
    }
  }

  public int getConflicts() {
    return conflicts;
  }
}
