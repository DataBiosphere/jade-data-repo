package bio.terra.service;

import bio.terra.stairway.Stairway;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class AsyncService {

    private Stairway stairway;
    private AsyncContext asyncContext;

    @Autowired
    public AsyncService(AsyncContext asyncContext) {
        this.asyncContext = asyncContext;

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        // TODO: datasource should be wired, and forceCleanStart should be true for local+dev, false o/w
        stairway = new Stairway(executorService, null, false, asyncContext);
    }

    public Stairway getStairway() {
        return stairway;
    }

    public AsyncContext getAsyncContext() {
        return asyncContext;
    }

}
