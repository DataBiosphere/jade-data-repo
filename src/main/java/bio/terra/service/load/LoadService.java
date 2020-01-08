package bio.terra.service.load;

import bio.terra.service.load.exception.LoadLockFailureException;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@Component
public class LoadService {
    private final LoadDao loadDao;

    @Autowired
    public LoadService(LoadDao loadDao) {
        this.loadDao = loadDao;
    }

    public void lockLoad(String loadTag, String flightId) {
        loadDao.lockLoad(loadTag, flightId);
    }

    public void unlockLoad(String loadTag, String flightId) {
        loadDao.unlockLoad(loadTag, flightId);
    }

    /**
     * @param inputTag  may be null or blank
     * @return either valid inputTag or generated date-time tag.
     */
    public String computeLoadTag(String inputTag) {
        if (StringUtils.isEmpty(inputTag)) {
            return "load-at-" + Instant.now().atZone(ZoneId.of("Z")).format(DateTimeFormatter.ISO_INSTANT);
        }
        return inputTag;
    }

    public String getLoadTag(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        String loadTag = inputParameters.get(LoadMapKeys.LOAD_TAG, String.class);
        if (StringUtils.isEmpty(loadTag)) {
            FlightMap workingMap = context.getWorkingMap();
            loadTag = workingMap.get(LoadMapKeys.LOAD_TAG, String.class);
            if (StringUtils.isEmpty(loadTag)) {
                throw new LoadLockFailureException("Expected LOAD_TAG in working map or inputs, but did not find it");
            }
        }
        return loadTag;
    }
}
