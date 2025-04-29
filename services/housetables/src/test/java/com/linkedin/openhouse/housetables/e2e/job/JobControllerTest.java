package com.linkedin.openhouse.housetables.e2e.job;

import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.api.spec.model.Job;
import com.linkedin.openhouse.housetables.api.spec.request.CreateUpdateEntityRequestBody;
import com.linkedin.openhouse.housetables.api.spec.response.GetAllEntityResponseBody;
import com.linkedin.openhouse.housetables.dto.mapper.JobMapper;
import com.linkedin.openhouse.housetables.model.JobRow;
import com.linkedin.openhouse.housetables.model.JobRowPrimaryKey;
import com.linkedin.openhouse.housetables.repository.HtsRepository;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.util.MultiValueMap;
import org.springframework.util.MultiValueMapAdapter;

@SpringBootTest
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
@AutoConfigureMockMvc
public class JobControllerTest {

  private final JobRow testJobRow =
      JobRow.builder()
          .jobId("id1")
          .state("Q")
          .version(1L)
          .jobName("jobName")
          .jobConf("jobConf")
          .clusterId("testCluster")
          .creationTimeMs(1651016746000L)
          .startTimeMs(1651016750000L)
          .finishTimeMs(1651017746000L)
          .lastUpdateTimeMs(1651017746000L)
          .heartbeatTimeMs(1651017746000L)
          .executionId("1")
          .engineType("LIVY")
          .retentionTimeSec(1745908497L)
          .build();

  @Autowired HtsRepository<JobRow, JobRowPrimaryKey> htsRepository;

  @Autowired MockMvc mvc;

  @Autowired JobMapper jobMapper;

  @BeforeEach
  public void setup() {
    htsRepository.save(testJobRow);
  }

  @AfterEach
  public void tearDown() {
    htsRepository.deleteAll();
  }

  @Test
  public void testFindAllJobs() throws Exception {
    JobRow testJobRow2 = htsRepository.save(testJobRow.toBuilder().jobId("id2").state("X").build());
    JobRow testJobRow3 = htsRepository.save(testJobRow.toBuilder().jobId("id3").state("X").build());

    Map<String, List<String>> paramsInternal = new HashMap<>();
    paramsInternal.put("state", Collections.singletonList("X"));
    MultiValueMap<String, String> params = new MultiValueMapAdapter(paramsInternal);
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/jobs/query")
                .params(params)
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(
            content()
                .json(
                    GetAllEntityResponseBody.builder()
                        .results(
                            Arrays.asList(
                                jobMapper.toJobDto(testJobRow2), jobMapper.toJobDto(testJobRow3)))
                        .build()
                        .toJson()));
  }

  @Test
  public void testFindJob() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/jobs")
                .param("jobId", testJobRow.getJobId())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.entity.jobId", is(equalTo(testJobRow.getJobId()))))
        .andExpect(jsonPath("$.entity.state", is(equalTo(testJobRow.getState()))))
        .andExpect(jsonPath("$.entity.jobName", is(equalTo(testJobRow.getJobName()))))
        .andExpect(jsonPath("$.entity.clusterId", is(equalTo(testJobRow.getClusterId()))))
        .andExpect(jsonPath("$.entity.executionId", is(equalTo(testJobRow.getExecutionId()))))
        .andExpect(jsonPath("$.entity.engineType", is(equalTo(testJobRow.getEngineType()))))
        .andExpect(jsonPath("$.entity.version", matchesPattern("\\d+")));
  }

  @Test
  public void testJobNotFound() throws Exception {
    mvc.perform(
            MockMvcRequestBuilders.get("/hts/jobs")
                .param("jobId", "notExist")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.status", is(equalTo(HttpStatus.NOT_FOUND.name()))))
        .andExpect(jsonPath("$.error", is(equalTo("Not Found"))))
        .andExpect(jsonPath("$.cause", notNullValue()))
        .andExpect(
            jsonPath(
                "$.message",
                is(
                    equalTo(
                        "$ent $id cannot be found"
                            .replace("$ent", "job")
                            .replace("$id", "notExist")))));
  }

  @Test
  public void testDeleteJob() throws Exception {
    mvc.perform(MockMvcRequestBuilders.delete("/hts/jobs").param("jobId", testJobRow.getJobId()))
        .andExpect(status().isNoContent())
        .andExpect(content().string(""));
  }

  @Test
  public void testDeleteNonExistedJob() throws Exception {
    mvc.perform(MockMvcRequestBuilders.delete("/hts/jobs").param("jobId", "notExist"))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.status", is(equalTo(HttpStatus.NOT_FOUND.name()))))
        .andExpect(jsonPath("$.error", is(equalTo("Not Found"))))
        .andExpect(jsonPath("$.cause", notNullValue()))
        .andExpect(
            jsonPath(
                "$.message",
                is(
                    equalTo(
                        "$ent $id cannot be found"
                            .replace("$ent", "job")
                            .replace("$id", "notExist")))));
  }

  @Test
  public void testPutJob() throws Exception {
    Job testJob =
        Job.builder()
            .jobId("id2")
            .state("X")
            .jobName("jobName")
            .clusterId("testCluster")
            .executionId("1")
            .engineType("LIVY")
            .build();
    // Create the job and return correct status code
    mvc.perform(
            MockMvcRequestBuilders.put("/hts/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<Job>builder().entity(testJob).build().toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isCreated())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.entity.jobId", is(equalTo(testJob.getJobId()))))
        .andExpect(jsonPath("$.entity.state", is(equalTo(testJob.getState()))))
        .andExpect(jsonPath("$.entity.jobName", is(equalTo(testJob.getJobName()))))
        .andExpect(jsonPath("$.entity.clusterId", is(equalTo(testJob.getClusterId()))))
        .andExpect(jsonPath("$.entity.executionId", is(equalTo(testJob.getExecutionId()))))
        .andExpect(jsonPath("$.entity.engineType", is(equalTo(testJobRow.getEngineType()))))
        .andExpect(jsonPath("$.entity.version", matchesPattern("\\d+")));

    // Update the job and returning the updated object.
    String atVersion =
        htsRepository
            .findById(JobRowPrimaryKey.builder().jobId(testJob.getJobId()).build())
            .get()
            .getVersion()
            .toString();
    Job modifiedJob = testJob.toBuilder().state("Y").version(atVersion).build();
    mvc.perform(
            MockMvcRequestBuilders.put("/hts/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<Job>builder()
                        .entity(modifiedJob)
                        .build()
                        .toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.entity.jobId", is(equalTo(modifiedJob.getJobId()))))
        .andExpect(jsonPath("$.entity.state", is(equalTo(modifiedJob.getState()))))
        .andExpect(jsonPath("$.entity.version", matchesPattern("\\d+")));
  }

  @Test
  public void testInvalidJobsHTSParams() throws Exception {
    Job testJob = Job.builder().executionId("1").build();
    mvc.perform(
            MockMvcRequestBuilders.put("/hts/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    CreateUpdateEntityRequestBody.<Job>builder().entity(testJob).build().toJson())
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.status", is(equalToIgnoringCase(HttpStatus.BAD_REQUEST.name()))))
        .andExpect(jsonPath("$.message", containsString("jobId cannot be empty")))
        .andExpect(jsonPath("$.message", containsString("jobName cannot be empty")))
        .andExpect(jsonPath("$.message", containsString("clusterId cannot be empty")))
        .andExpect(jsonPath("$.error", is(equalTo(HttpStatus.BAD_REQUEST.getReasonPhrase()))));
  }
}
