package com.example.fdepservice;

import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import static org.springframework.http.MediaType.APPLICATION_JSON;

@Service
public class FdepServiceService {

    private final DiscoveryClient discoveryClient;
    private final RestClient restClient;

    public FdepServiceService(DiscoveryClient discoveryClient, RestClient.Builder restClientBuilder) {

        this.discoveryClient = discoveryClient;
        this.restClient = restClientBuilder.build();
    }

    @Async
    public void startJob(Long jobId) {
        ServiceInstance serviceInstanceJob = discoveryClient.getInstances("jobservice").get(0);

        updateStatus(jobId, JobStatus.RUNNING, serviceInstanceJob);

        try {

            JobDto job = restClient.get()
                    .uri(serviceInstanceJob.getUri() + "/jobs/" + jobId)
                    .accept(APPLICATION_JSON)
                    .retrieve()
                    .body(JobDto.class);

            ServiceInstance serviceInstanceData = discoveryClient.getInstances("dataservice").get(0);
            DatasetDto dataset = restClient.get()
                    .uri(serviceInstanceData.getUri() + "/datasets/" + job.getDataset())
                    .accept(APPLICATION_JSON)
                    .retrieve()
                    .body(DatasetDto.class);

            // TODO spustit ALG FDEP

            Thread.sleep(10000);

            updateStatus(jobId, JobStatus.DONE, serviceInstanceJob);
        }
        catch (Exception e) {
            System.err.println("Error starting job " + jobId + ": " + e.getMessage());
            updateStatus(jobId, JobStatus.FAILED, serviceInstanceJob);
        }
    }

    private void updateStatus(Long jobId, JobStatus status, ServiceInstance serviceInstanceJob) {
        ResponseEntity<Void> response = restClient.patch()
                .uri(serviceInstanceJob.getUri() + "/jobs/" + jobId + "/status")
                .contentType(APPLICATION_JSON)
                .body(status)
                .retrieve()
                .toBodilessEntity();

    }


}
