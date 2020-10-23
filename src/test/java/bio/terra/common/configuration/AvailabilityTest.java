package bio.terra.common.configuration;

import bio.terra.common.category.Connected;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.boot.availability.LivenessState;
import org.springframework.boot.availability.ApplicationAvailability;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.context.ApplicationContext;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest
@Category(Connected.class)
public class AvailabilityTest {

    @Autowired
    private MockMvc mvc;
    @Autowired
    private ApplicationContext context;
    @Autowired
    private ApplicationAvailability applicationAvailability;

    @Test
    public void readinessState() throws Exception {
        assertThat("Readiness state should be ACCEPTING_TRAFFIC",
            applicationAvailability.getReadinessState(),
            equalTo(ReadinessState.ACCEPTING_TRAFFIC));
        ResultActions result = mvc.perform(get("/actuator/health/liveness"));
        result.andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("UP"));
        ResultActions readinessResult = mvc.perform(get("/actuator/health/readiness"));
        readinessResult.andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("UP"));

        AvailabilityChangeEvent.publish(context, ReadinessState.REFUSING_TRAFFIC);
        assertThat("Readiness state should be REFUSING_TRAFFIC",
            applicationAvailability.getReadinessState(),
            equalTo(ReadinessState.REFUSING_TRAFFIC));
        mvc.perform(get("/actuator/health/readiness"))
           .andExpect(status().isServiceUnavailable())
           .andExpect(jsonPath("$.status").value("OUT_OF_SERVICE"));
    }
}
