package bio.terra.common.configuration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Connected;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.availability.ApplicationAvailability;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.LivenessState;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@SpringBootTest
@Category(Connected.class)
@EmbeddedDatabaseTest
public class AvailabilityTest {

  @Autowired private MockMvc mvc;
  @Autowired private ApplicationContext context;
  @Autowired private ApplicationAvailability applicationAvailability;

  // Reference: https://www.baeldung.com/spring-liveness-readiness-probes
  @Test
  public void readinessState() throws Exception {
    assertThat(
        "Readiness state should be ACCEPTING_TRAFFIC",
        applicationAvailability.getReadinessState(),
        equalTo(ReadinessState.ACCEPTING_TRAFFIC));
    ResultActions readinessResult = mvc.perform(get("/actuator/health/readiness"));
    readinessResult.andExpect(status().isOk()).andExpect(jsonPath("$.status").value("UP"));

    AvailabilityChangeEvent.publish(context, ReadinessState.REFUSING_TRAFFIC);
    assertThat(
        "Readiness state should be REFUSING_TRAFFIC",
        applicationAvailability.getReadinessState(),
        equalTo(ReadinessState.REFUSING_TRAFFIC));
    mvc.perform(get("/actuator/health/readiness"))
        .andExpect(status().isServiceUnavailable())
        .andExpect(jsonPath("$.status").value("OUT_OF_SERVICE"));
  }

  @Test
  public void livenessState() throws Exception {
    assertThat(
        "Liveness state should be CORRECT",
        applicationAvailability.getLivenessState(),
        equalTo(LivenessState.CORRECT));
    ResultActions result = mvc.perform(get("/actuator/health/liveness"));
    result.andExpect(status().isOk()).andExpect(jsonPath("$.status").value("UP"));
    ResultActions livenessResult = mvc.perform(get("/actuator/health/liveness"));
    livenessResult.andExpect(status().isOk()).andExpect(jsonPath("$.status").value("UP"));

    AvailabilityChangeEvent.publish(context, LivenessState.BROKEN);
    assertThat(
        "Liveness state should be BROKEN",
        applicationAvailability.getLivenessState(),
        equalTo(LivenessState.BROKEN));
    mvc.perform(get("/actuator/health/liveness"))
        .andExpect(status().isServiceUnavailable())
        .andExpect(jsonPath("$.status").value("DOWN"));
  }
}
