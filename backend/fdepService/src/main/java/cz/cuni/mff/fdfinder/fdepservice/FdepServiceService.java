package cz.cuni.mff.fdfinder.fdepservice;

import cz.cuni.mff.algorithms.fdep_spark.FdepSpark;
import cz.cuni.mff.algorithms.fdep_spark.model._FunctionalDependency;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.*;
import java.util.List;
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

                JobResultsFDs jb = runJob(currentJobId);
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

    public record JobResultsFDs(

            JobResult jobResult,
            List<_FunctionalDependency> foundFds
    ) implements Serializable {};

    private JobResultsFDs runJob(Long jobId) {
        ServiceInstance serviceInstanceJob = discoveryClient.getInstances("jobservice").get(0);
        JobResult jobResult = new JobResult(jobId);
        JobResultsFDs jobResultsFDs;
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
            Path targetPathDataset = Paths.get("tmp/datasets/job-" + jobId + "." + dataset.getFileFormat().toString().toLowerCase());
            Files.createDirectories(targetPathDataset.getParent());


            Resource file = response.getBody();
            if (file != null) {

                try (InputStream in = file.getInputStream()) {

                    Files.copy(in, targetPathDataset, StandardCopyOption.REPLACE_EXISTING);
                }
            }
            else {
                throw new IOException("Dataset is empty");
            }

            jobResult.setStartTime(System.currentTimeMillis());

            System.out.println("FDEP - JOB started at TIME: " + jobResult.startTime);

            // TODO metriky algoritmu

            FdepSpark algorithm = new FdepSpark();
            List<_FunctionalDependency> foundFds = algorithm.startAlgorithm(targetPathDataset, dataset.getName(), job.getSkipEntries(), job.getLimitEntries(), job.getMaxLHS(), dataset.getFileFormat(), dataset.getHeader(), dataset.getDelim());

            jobResult.setEndTime(System.currentTimeMillis());
            algorithm = null;

            System.out.println("FDEP - JOB Spark finished at TIME: " + jobResult.endTime);

            Files.deleteIfExists(targetPathDataset);
            jobResultsFDs = new JobResultsFDs(jobResult, foundFds);

            updateStatus(jobId, JobStatus.DONE, serviceInstanceJob);

            return jobResultsFDs;
        }
        catch (IOException e){
            System.err.println("IOEX Error starting job " + jobId + ": " + e.getMessage());
            updateStatus(jobId, JobStatus.FAILED, serviceInstanceJob);
        }
        catch (Exception e) {
            System.err.println("Error starting job " + jobId + ": " + e.getMessage());
            updateStatus(jobId, JobStatus.FAILED, serviceInstanceJob);
        }

        return null;
    }

    private void processResult(JobResultsFDs jobResultsFDs) {

        // TODO vystupne FD ulozit do suboru a poslat na JOBService
        System.out.println("FDEP Processing result of job");
        if (jobResultsFDs == null) {

            return;
        }

        try{
            jobResultsFDs.jobResult.numFoundFd = jobResultsFDs.foundFds.size();

            Path targetPathResult = getFdsResultFilePath(jobResultsFDs.jobResult.jobId);
            Files.createDirectories(targetPathResult.getParent());

            Files.createFile(targetPathResult);

            FileWriter writer = new  FileWriter(getFdsResultFilePath(jobResultsFDs.jobResult.jobId).toFile());

            for (_FunctionalDependency fd : jobResultsFDs.foundFds) {

                writer.write(fd.toString()+"\n");
            }
            writer.close();

            sendResultsToJobService(jobResultsFDs.jobResult);

            System.out.println("FDEP Processing finished successfully");
        }
        catch (FileAlreadyExistsException e){
            System.out.println("FDEP Processing already exists");
        }
        catch (IOException e){
            System.out.println("FDEP Processing failed" + e.getMessage());
        }

    }

    private void sendResultsToJobService(JobResult  jobResult) {
        Path path = getFdsResultFilePath(jobResult.jobId);

        if(!Files.exists(path)) {
            System.out.println("FDEP - ERROR: " + path + " does not exist");
            return;
        }

        try {
            Resource fileResource = new UrlResource(path.toUri());

            MultipartBodyBuilder builder = new MultipartBodyBuilder();
            builder.part("jobresult", jobResult)
                    .contentType(MediaType.APPLICATION_JSON);
            builder.part("file", fileResource);


            ServiceInstance serviceInstanceJob = discoveryClient.getInstances("jobservice").get(0);
            String response = restClient.post()
                    .uri(serviceInstanceJob.getUri() + "/jobs/" + jobResult.jobId + "/results")
                    .contentType(MediaType.MULTIPART_FORM_DATA)
                    .body(builder.build())
                    .retrieve()
                    .body(String.class);

            System.out.println("FDEP - JOB SEND SUCCESSFULLY: " + response);

            Files.deleteIfExists(path);
        }
        catch (IOException e){
            System.out.println("FDEP - JOB SENDING ERROR: " + e.getMessage());
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

    private Path getFdsResultFilePath(Long jobId){

        return Paths.get("tmp/results/job-" + jobId + "-FDs.txt");
    }


}
