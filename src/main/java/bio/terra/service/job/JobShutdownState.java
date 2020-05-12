package bio.terra.service.job;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Several components need to access the job shutdown state. To avoid infinite
 * dependency loops, we factor the state into its own component.
 */
@Component
public class JobShutdownState {
    private final AtomicBoolean isShutdown;

    public JobShutdownState() {
        this.isShutdown = new AtomicBoolean(false);
    }

    void clearShutdown() {
        isShutdown.set(false);
    }

    void setShutdown() {
        isShutdown.set(true);
    }

    public boolean isShutdown() {
        return isShutdown.get();
    }
}
