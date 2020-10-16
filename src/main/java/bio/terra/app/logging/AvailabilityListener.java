package bio.terra.app.logging;

import org.springframework.boot.availability.*;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
public class AvailabilityListener {
    private static Logger logger = LoggerFactory.getLogger(AvailabilityListener.class);

    @EventListener
    public void onLivenessEvent(AvailabilityChangeEvent<LivenessState> event) {
        logger.info("AvailabilityChangeEvent - LivenessState: {}", event.getState());
        switch (event.getState()) {
            case BROKEN:
                // notify others
                break;
            case CORRECT:
                // we're back
        }
    }

    @EventListener
    public void onReadinessEvent(AvailabilityChangeEvent<ReadinessState> event) {
        logger.info("AvailabilityChangeEvent - ReadinessState: {}", event.getState());
    }
}
