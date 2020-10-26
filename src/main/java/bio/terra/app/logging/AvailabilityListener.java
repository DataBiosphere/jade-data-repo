package bio.terra.app.logging;

import org.springframework.boot.availability.LivenessState;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
public class AvailabilityListener {
    private static Logger logger = LoggerFactory.getLogger(AvailabilityListener.class);

    @EventListener
    public void onLivenessEvent(AvailabilityChangeEvent<LivenessState> event) {
        switch (event.getState()) {
            case BROKEN:
                logger.error("LivenessState: {}", event.getState());
                break;
            case CORRECT:
                logger.debug("LivenessState: {}", event.getState());
                break;
            default:
                logger.error("LivenessState {} is not a known state", event.getState());
        }
    }

    @EventListener
    public void onReadinessEvent(AvailabilityChangeEvent<ReadinessState> event) {
        switch (event.getState()) {
            case REFUSING_TRAFFIC:
                logger.error("ReadinessState: {}", event.getState());
                break;
            case ACCEPTING_TRAFFIC:
                logger.debug("ReadinessState: {}", event.getState());
                break;
            default:
                logger.error("ReadinessState {} is not a known state", event.getState());
        }
    }
}
