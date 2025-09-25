package cz.cuni.mff.fdfinder.fdepservice;

import cz.cuni.mff.algorithms.fdep_spark.FdepSpark;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.core.io.Resource;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.springframework.http.MediaType.APPLICATION_JSON;

@Service
public class FdepServiceService {

    private final DiscoveryClient discoveryClient;
    private final RestClient restClient;

    private final Queue<Long> queue = new ConcurrentLinkedQueue<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private volatile Future<?> currentJob;
    private volatile Long currentJobId;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public FdepServiceService(DiscoveryClient discoveryClient, RestClient.Builder restClientBuilder) {

        this.discoveryClient = discoveryClient;
        this.restClient = restClientBuilder.build();
    }

    public void registerNewJob(Long jobId) {

        this.queue.offer(jobId);
        System.out.println("FDEP - QUEUE: " + this.queue.size());
        if (!running.get()) {
            startNext();
        }
    }

    private void startNext() {

        Long id = this.queue.poll();
        System.out.println("FDEP - ID poll: " + id);
        if (id == null) {

            currentJobId = null;
            currentJob = null;
            return;
        }

        running.set(true);
        currentJobId = id;
        currentJob = executor.submit(() -> {
            try {
                System.out.println("FDEP - STARTING JOB: " + id);

                JobResult jb = runJob(currentJobId);
                processResult(jb);
            }
            catch (Throwable t) {
                System.out.println("FDEP - ERROR: " + t.getMessage());
                t.printStackTrace();
            }
            finally {
                System.out.println("FDEP - FINISHED JOB: " + id);

                running.set(false);
                currentJobId = null;
                currentJob = null;
                startNext();
            }
        });
    }

    private JobResult runJob(Long jobId) {
        ServiceInstance serviceInstanceJob = discoveryClient.getInstances("jobservice").get(0);
        JobResult jobResult = new JobResult(jobId);
        updateStatus(jobId, JobStatus.RUNNING, serviceInstanceJob);
        System.out.println("FDEP - JOB running: " + jobId);
        try {

            // get metadata about JOB
            JobDto job = restClient.get()
                    .uri(serviceInstanceJob.getUri() + "/jobs/" + jobId)
                    .accept(APPLICATION_JSON)
                    .retrieve()
                    .body(JobDto.class);
            System.out.println("FDEP - JOB retrieved = job ");

            // get metadata about DATASET
            ServiceInstance serviceInstanceData = discoveryClient.getInstances("dataservice").get(0);
            DatasetDto dataset = restClient.get()
                    .uri(serviceInstanceData.getUri() + "/datasets/" + job.getDataset())
                    .accept(APPLICATION_JSON)
                    .retrieve()
                    .body(DatasetDto.class);
            System.out.println("FDEP - JOB retrieved = dataset ");

            // download DATASET content
            ResponseEntity<Resource> response = restClient.get()
                    .uri(serviceInstanceData.getUri() + "/datasets/" + dataset.getId() + "/file")
                    .retrieve()
                    .toEntity(Resource.class);
            System.out.println("FDEP - JOB retrieved = file ");

            // store DATASET locally
            Path targetPath = Paths.get("datasets/tmp/job-" + jobId + "." + dataset.getFileFormat().toString().toLowerCase());
            Files.createDirectories(targetPath.getParent());

            Resource file = response.getBody();
            if (file != null) {

                try (InputStream in = file.getInputStream()) {

                    Files.copy(in, targetPath, StandardCopyOption.REPLACE_EXISTING);
                }
            }
            else {
                throw new IOException("Dataset is empty");
            }

            jobResult.setStartTime(System.currentTimeMillis());

            System.out.println("FDEP - JOB started at TIME: " + jobResult.startTime);

            // TODO spustit ALG FDEP
            // TODO vysledne FD algoritmu
            // TODO metriky algoritmu

            FdepSpark algorithm = new FdepSpark();
            algorithm.startAlgorithm(targetPath, job.getSkipEntries(), job.getMaxEntries(), job.getMaxLHS(), dataset.getFileFormat(), dataset.getHeader(), dataset.getDelim());

            jobResult.setEndTime(System.currentTimeMillis());
            algorithm = null;

            System.out.println("FDEP - JOB Spark finished at TIME: " + jobResult.endTime);

            Files.deleteIfExists(targetPath);

            updateStatus(jobId, JobStatus.DONE, serviceInstanceJob);
        }
        catch (IOException e){
            System.err.println("IOEX Error starting job " + jobId + ": " + e.getMessage());
            updateStatus(jobId, JobStatus.FAILED, serviceInstanceJob);
        }
        catch (Exception e) {
            System.err.println("Error starting job " + jobId + ": " + e.getMessage());
            updateStatus(jobId, JobStatus.FAILED, serviceInstanceJob);
        }

        return jobResult;
    }

    private void processResult(JobResult jobResult) {

        // TODO vystupne FD ulozit do suboru a poslat na JOBService
        System.out.println("FDEP Processing result of job");
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
