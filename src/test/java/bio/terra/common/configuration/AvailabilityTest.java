package bio.terra.common.configuration;

import bio.terra.common.*;
import bio.terra.common.category.*;
import org.junit.*;
import org.junit.experimental.categories.*;
import org.junit.runner.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.boot.availability.*;
import org.springframework.boot.test.autoconfigure.web.servlet.*;
import org.springframework.boot.test.context.*;
import org.springframework.http.*;
import org.springframework.mock.web.*;
import org.springframework.test.context.junit4.*;
import org.springframework.test.web.servlet.*;

import static org.junit.Assert.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest
@Category(Connected.class)
public class AvailabilityTest {

    @Autowired
    ApplicationAvailability applicationAvailability;

    @Autowired
    private MockMvc mvc;

   @Test
   public void test() throws Exception {
       /*ReadinessState state = applicationAvailability.getReadinessState();
       assertThat(state)
           .isEqualTo(ReadinessState.ACCEPTING_TRAFFIC);*/
       ResultActions result = mvc.perform(get("/actuator/health"));
       result.andExpect(status().isOk())
           .andExpect(jsonPath("$.status").value("UP"));


       /*AvailabilityChangeEvent.publish(context, ReadinessState.REFUSING_TRAFFIC);

       assertThat(applicationAvailability.getReadinessState())
           .isEqualTo(ReadinessState.REFUSING_TRAFFIC);
       mockMvc.perform(get("/actuator/health/readiness"))
           .andExpect(status().isServiceUnavailable())
           .andExpect(jsonPath("$.status").value("OUT_OF_SERVICE"));*/
   }
}
