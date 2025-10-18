package cz.cuni.mff.fdfinder.fdepservice.service;

import cz.cuni.mff.fdfinder.fdepservice.algorithm.FdepSpark;
import cz.cuni.mff.fdfinder.fdepservice.algorithm.model._FunctionalDependency;
import cz.cuni.mff.fdfinder.fdepservice.model.DatasetDto;
import cz.cuni.mff.fdfinder.fdepservice.model.JobDto;
import cz.cuni.mff.fdfinder.fdepservice.model.JobResult;
import cz.cuni.mff.fdfinder.fdepservice.model.JobStatus;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.springframework.http.MediaType.APPLICATION_JSON;

@Service
public class FdepService {

    private final DiscoveryClient discoveryClient;
    private final RestClient restClient;

    private final Queue<JobDto> queue = new ConcurrentLinkedQueue<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private volatile Future<?> currentJobFuture;
    private volatile JobDto currentJob;
    private String actualSparkGroupId;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    private final JobMonitorService monitorService = new JobMonitorService();
    private final SparkSession spark;
    private final JavaSparkContext sparkContext;

    public FdepService(DiscoveryClient discoveryClient, RestClient.Builder restClientBuilder) {

        this.discoveryClient = discoveryClient;
        this.restClient = restClientBuilder.build();

        spark = SparkSession.builder()
                .appName("FDEP-spark")
                .master("local")
                .getOrCreate();
        sparkContext = new JavaSparkContext(spark.sparkContext());
    }

    public void registerNewJob(JobDto job) {

        this.queue.offer(job);
        System.out.println("FDEP - QUEUE: " + this.queue.size());
        if (!running.get()) {
            startNext();
        }
    }

    public void cancelJob(long jobId) {
        System.out.println("TRYING to CANCEL JOB: " + jobId);

        if (this.currentJob != null && this.currentJob.getId() == jobId) {
            cancelled.set(true);
            sparkContext.sc().cancelJobGroupAndFutureJobs(this.actualSparkGroupId);
            System.out.println("FDEP - CANCEL Job: " + this.currentJob.getId());
            System.out.println("FDEP - CANCEL queue size: " + this.queue.size());
        }
        else {
            for (JobDto job : this.queue) {

                if (job.getId() == jobId) {
                    System.out.println("FDEP - CANCEL Job = removed from queue: " + job.getId());
                    this.queue.remove(job);
                    break;
                }
            }
        }

        System.out.println("Job CANCEL " + jobId + " DONE");
    }

    private void startNext() {

        JobDto job = this.queue.poll();
        if (job == null) {

            currentJob = null;
            currentJobFuture = null;
            return;
        }
        System.out.println("FDEP - ID poll: " + job.getId());

        running.set(true);
        currentJob = job;
        currentJobFuture = executor.submit(() -> {
            try {
                System.out.println("FDEP - STARTING JOB: " + job.getId());
                this.actualSparkGroupId = getSparkContextGroupId(job.getId());
                sparkContext.setJobGroup(this.actualSparkGroupId, "FDEP spark new job", true);
                runJob(currentJob);
            }
            catch (Throwable t) {
                System.out.println("FDEP - ERROR: " + t.getMessage());
                t.printStackTrace();
            }
            finally {
                System.out.println("FDEP - FINISHED JOB: " + job.getId());
                sparkContext.clearJobGroup();
                running.set(false);
                cancelled.set(false);
                currentJob = null;
                currentJobFuture = null;
                startNext();
            }
        });
    }

    public record JobResultsFDs(

            JobResult jobResult,
            List<_FunctionalDependency> foundFds
    ) implements Serializable {};

    private List<JobResultsFDs> runJob(JobDto job) {
        ServiceInstance serviceInstanceJob = discoveryClient.getInstances("jobservice").getFirst();

        List<JobResultsFDs> jobResultsFDs = generateOutputJobResults(job.getJobResults());
        JobResult currentJobResult = null;
        DatasetData datasetData = null;

        try {
            System.out.println("Downloading dataset...");
            datasetData = getMetadataAndDownloadDataset(job);



            for (JobResult jobResult : job.getJobResults()) {
                currentJobResult = jobResult;

                updateStatus(jobResult.getId(), JobStatus.RUNNING, serviceInstanceJob);
                System.out.println("FDEP - subJOB running: " + currentJobResult.getId());

                FdepSpark algorithm = new FdepSpark(sparkContext, spark, datasetData.targetPathDataset,
                        datasetData.dataset.getName(), job.getSkipEntries(), job.getLimitEntries(), job.getMaxLHS(),
                        datasetData.dataset.getFileFormat(), datasetData.dataset.getHeader(), datasetData.dataset.getDelim());

                jobResult.setStartTime(System.currentTimeMillis());
                System.out.println("FDEP - subJOB started at TIME: " + jobResult.getStartTime());
                monitorService.startMonitoring(jobResult.getId());

                List<_FunctionalDependency> foundFds = algorithm.startAlgorithm();

                monitorService.stopMonitoring(jobResult.getId());
                jobResult.setEndTime(System.currentTimeMillis());
                jobResult.setSnapshots(monitorService.getSnapshots(jobResult.getId()));

                System.out.println("FDEP - subJOB Spark finished at TIME: " + jobResult.getEndTime());

                processOneJobResult(new JobResultsFDs(jobResult, foundFds), job.getId());
                updateStatus(jobResult.getId(), JobStatus.DONE, serviceInstanceJob);

            }

            Files.deleteIfExists(datasetData.targetPathDataset);

            return jobResultsFDs;
        }
        catch (IOException e){

            System.err.println("IOEX Error starting job " + currentJob.getId() + ": " + e.getMessage());
            updateStatus(currentJob.getJobResults().getFirst().getId(), JobStatus.FAILED, serviceInstanceJob);

        }
        catch (Exception e) {

            if (cancelled.get() && e instanceof SparkException) {

                System.out.println("FDEP - cancelled Spark Job: " + e.getMessage());

            }
            else if (currentJobResult != null) {
                System.err.println("Error starting job " + currentJobResult.getId() + ": " + e.getMessage());
                updateStatus(currentJobResult.getId(), JobStatus.FAILED, serviceInstanceJob);
            }

        }
        finally {

            deleteDataset(datasetData);
        }

        return null;
    }

    public record DatasetData(

            DatasetDto dataset,
            Path targetPathDataset
    ) implements Serializable {};

    private DatasetData getMetadataAndDownloadDataset(JobDto job) throws IOException {
        // get metadata about DATASET
        ServiceInstance serviceInstanceData = discoveryClient.getInstances("dataservice").getFirst();
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
        Path targetPathDataset = Paths.get("tmp/datasets/job-" + job.getId() + "." + dataset.getFileFormat().toString().toLowerCase());
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

        return new DatasetData(dataset, targetPathDataset);
    }

    private void deleteDataset(DatasetData datasetData) {

        if (datasetData == null) return;

        try {
            Files.deleteIfExists(datasetData.targetPathDataset);
        }
        catch (IOException e) {

        }
    }

    private List<JobResultsFDs> generateOutputJobResults(List<JobResult> jobResults) {

        List<JobResultsFDs> jobResultsFDs = new ArrayList<>();

        for (JobResult jobResult : jobResults) {

            jobResultsFDs.add(new JobResultsFDs(jobResult, new LinkedList<>()));
        }

        return jobResultsFDs;
    }

    @Async
    protected void processOneJobResult(JobResultsFDs jobResultsFDs, long jobId) {

        System.out.println("FDEP Processing result of ONE JOB");
        if (jobResultsFDs == null) {

            return;
        }

        try{
            Path foundFdsFilePath = saveFoundFdsToResultFile(jobResultsFDs, jobId);

            sendResultsToJobService(jobResultsFDs.jobResult, foundFdsFilePath);

            System.out.println("FDEP Processing finished successfully");
        }
        catch (FileAlreadyExistsException e){
            System.out.println("FDEP Processing already exists");
        }
        catch (IOException e){
            System.out.println("FDEP Processing failed" + e.getMessage());
        }
        finally {
            monitorService.deleteSnapshots(jobResultsFDs.jobResult.getId());
        }
    }

    private void processResults(List<JobResultsFDs> jobResultsFDs, long jobId) {

        // TODO vystupne FD ulozit do suboru a poslat na JOBService
        System.out.println("FDEP Processing result of job");
        if (jobResultsFDs == null || jobResultsFDs.isEmpty()) {

            return;
        }

        try{

            for (JobResultsFDs jobResultsFD : jobResultsFDs) {

                Path foundFdsFilePath = saveFoundFdsToResultFile(jobResultsFD,  jobId);

                sendResultsToJobService(jobResultsFD.jobResult, foundFdsFilePath);
            }

            System.out.println("FDEP Processing finished successfully");
        }
        catch (FileAlreadyExistsException e){
            System.out.println("FDEP Processing already exists");
        }
        catch (IOException e){
            System.out.println("FDEP Processing failed" + e.getMessage());
        }

    }


    private Path saveFoundFdsToResultFile(JobResultsFDs jobResultsFDs, long jobId) throws IOException {

        jobResultsFDs.jobResult.setNumFoundFd(jobResultsFDs.foundFds.size());

        Path targetPathResult = getFdsResultFilePath(jobResultsFDs.jobResult, jobId);

        Files.createDirectories(targetPathResult.getParent());
        Files.createFile(targetPathResult);

        FileWriter writer = new  FileWriter(targetPathResult.toFile());

        for (_FunctionalDependency fd : jobResultsFDs.foundFds) {

            writer.write(fd.toString()+"\n");
        }
        writer.close();

        return targetPathResult;
    }

    private void sendResultsToJobService(JobResult jobResult, Path foundFdsFilePath) {

        if(!Files.exists(foundFdsFilePath)) {
            System.out.println("FDEP - ERROR: " + foundFdsFilePath + " does not exist");
            return;
        }

        try {
            Resource fileResource = new UrlResource(foundFdsFilePath.toUri());

            MultipartBodyBuilder builder = new MultipartBodyBuilder();
            builder.part("jobresult", jobResult)
                    .contentType(MediaType.APPLICATION_JSON);
            builder.part("file", fileResource);


            ServiceInstance serviceInstanceJob = discoveryClient.getInstances("jobservice").getFirst();
            String response = restClient.patch()
                    .uri(serviceInstanceJob.getUri() + "/jobs/results/" + jobResult.getId())
                    .contentType(MediaType.MULTIPART_FORM_DATA)
                    .body(builder.build())
                    .retrieve()
                    .body(String.class);

            System.out.println("FDEP - JOB SEND SUCCESSFULLY: " + response);

            Files.deleteIfExists(foundFdsFilePath);
        }
        catch (IOException e){
            System.out.println("FDEP - JOB SENDING ERROR: " + e.getMessage());
        }



    }

    // update status of ONE JOB ITERATION -> JOB status is updated/computed automaticaly in JOB SERVICE
    private void updateStatus(Long jobResultId, JobStatus status, ServiceInstance serviceInstanceJob) {
        ResponseEntity<Void> response = restClient.patch()
                .uri(serviceInstanceJob.getUri() + "/jobs/iteration/" + jobResultId + "/status")
                .contentType(APPLICATION_JSON)
                .body(status)
                .retrieve()
                .toBodilessEntity();

    }

    private Path getFdsResultFilePath(JobResult jobResult, long jobId) {
        // fileName: job-ID-fdep-run-#-foundFDs.txt
        return Paths.get("tmp/results/job-" + jobId
                + "-fdep-run-" + jobResult.getIteration() + "-foundFDs.txt");
    }

    private String getSparkContextGroupId(long jobId) {

        return "job"+jobId+"_"+ UUID.randomUUID();
    }
}
