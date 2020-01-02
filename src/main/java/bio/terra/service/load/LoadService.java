package bio.terra.service.load;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@Component
public class LoadService {
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


    /*
    Entrypoints for steps:
    lock load tag - will create the tag if it doesn't exist
    unlock load tag

    Add steps for use in dataset and file flights
     */



}
