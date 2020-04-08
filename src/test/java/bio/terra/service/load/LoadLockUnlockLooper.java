package bio.terra.service.load;

import bio.terra.service.load.exception.LoadLockedException;

public class LoadLockUnlockLooper implements Runnable {
    private LoadDao loadDao;
    private String loadTag;
    private String flightId;
    private int count;
    private int conflicts;
    private int sqlerrors;

    public LoadLockUnlockLooper(LoadDao loadDao, String loadTag, String flightId, int count) {
        this.loadDao = loadDao;
        this.loadTag = loadTag;
        this.flightId = flightId;
        this.count = count;
    }

    @Override
    public void run() {
        conflicts = 0;
        sqlerrors = 0;
        for (int i = 0; i < count; ) {
            try {
                loadDao.lockLoad(loadTag, flightId);
                loadDao.unlockLoad(loadTag, flightId);
                i++;
            } catch (LoadLockedException ex) {
                conflicts++;
            }
        }
    }

    public int getConflicts() {
        return conflicts;
    }


}
