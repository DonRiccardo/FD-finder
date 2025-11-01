package cz.cuni.mff.fdfinder.fdepservice.service;

import cz.cuni.mff.fdfinder.fdepservice.algorithm.FdepSpark;
import de.metanome.algorithms.depminer.depminer_helper.modules.container._FunctionalDependency;
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

/**
 * Service for algorithm doing preparing, setting and running the algorithm.
 */
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

    /**
     * Register a new job and starts it if nothing is running.
     * @param job {@link JobDto} ready to run
     */
    public void registerNewJob(JobDto job) {

        this.queue.offer(job);
        System.out.println("FDEP - QUEUE: " + this.queue.size());
        if (!running.get()) {
            startNext();
        }
    }

    /**
     * Cancel a specified job. Kill it if the job is running.
     * @param jobId {@link Long} job ID to cancel
     */
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

    /**
     * Start running a new job from the queue.
     */
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
                //t.printStackTrace();
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

    /**
     * Run a specified job.
     * Downloads dataset, start algorithm, store results, send results, delete results and dataset.
     * @param job {@link JobDto} to run
     */
    private void runJob(JobDto job) {
        ServiceInstance serviceInstanceJob = discoveryClient.getInstances("jobservice").getFirst();

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

                processOneJobResult(jobResult, foundFds, job.getId());
                updateStatus(jobResult.getId(), JobStatus.DONE, serviceInstanceJob);

            }

            Files.deleteIfExists(datasetData.targetPathDataset);

        }
        catch (IOException e){
            // dataset was empty
            System.err.println("IOEX Error starting job " + currentJob.getId() + ": " + e.getMessage());
            updateStatus(currentJob.getJobResults().getFirst().getId(), JobStatus.FAILED, serviceInstanceJob);

        }
        catch (Exception e) {

            if (cancelled.get() && e instanceof SparkException) {
                // thrown if job is canceled while running
                System.out.println("DEP-MINER - cancelled Spark Job: " + e.getMessage());

            }
            else if (currentJobResult != null) {
                // some job iteration throws exception
                System.err.println("Error starting job, current result ID: " + currentJobResult.getId() + ": " + e.getMessage());
                updateStatus(currentJobResult.getId(), JobStatus.FAILED, serviceInstanceJob);
            }
            else {
                // not possible to start the job
                System.err.println("Error starting job " + currentJob.getId() + ": " + e.getMessage());
                for (JobResult jobResult : job.getJobResults()) {
                    updateStatus(jobResult.getId(), JobStatus.FAILED, serviceInstanceJob);
                }
            }

        }
        finally {

            deleteDataset(datasetData);
            if (currentJobResult != null) monitorService.stopMonitoring(currentJobResult.getId());
        }

    }

    /**
     * Record to store dataset metadata and path to the dataset.
     * @param dataset {@link DatasetDto} data
     * @param targetPathDataset {@link Path} to the dataset
     */
    public record DatasetData(

            DatasetDto dataset,
            Path targetPathDataset
    ) implements Serializable {};

    /**
     * Get metadata and download dataset from DataService.
     * Store dataset locally.
     * @param job {@link JobDto} to run
     * @return {@link DatasetData} data
     * @throws IOException if dataset is empty
     */
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
        Path targetPathDataset = Paths.get("datatmp/datasets/job-" + job.getId() + "." + dataset.getFileFormat().toString().toLowerCase());
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

    /**
     * Deletes dataset used to finding FDs.
     * Use only after all job iterations are DONE.
     * @param datasetData {@link DatasetData} data
     */
    private void deleteDataset(DatasetData datasetData) {

        if (datasetData == null) return;

        try {
            Files.deleteIfExists(datasetData.targetPathDataset);
        }
        catch (IOException e) {

        }
    }

    /**
     * Fully process results of one job iteration.
     * Save found FDs to a file, send results to JobService (with deletion of a file of found FDs).
     * @param jobResult  {@link JobResult} data from one job iteration
     * @param fds List ({@link _FunctionalDependency}) found FDs in the dataset
     * @param jobId {@link Long} id of a {@link JobDto}
     */
    @Async
    protected void processOneJobResult(JobResult jobResult, List<_FunctionalDependency> fds, long jobId) {

        System.out.println("FDEP Processing result of ONE JOB");
        if (jobResult == null) {

            return;
        }

        try{
            Path foundFdsFilePath = saveFoundFdsToResultFile(jobResult, fds, jobId);

            sendResultsToJobService(jobResult, foundFdsFilePath);

            System.out.println("FDEP Processing finished successfully");
        }
        catch (FileAlreadyExistsException e){
            System.out.println("FDEP Processing already exists");
        }
        catch (IOException e){
            System.out.println("FDEP Processing failed" + e.getMessage());
        }
        finally {
            monitorService.deleteSnapshots(jobResult.getId());
        }
    }

    /**
     * Save found FDs of one job iteration to a file.
     * @param jobResult  {@link JobResult} data from one job iteration
     * @param fds List ({@link _FunctionalDependency}) found FDs in the dataset
     * @param jobId {@link Long} id of a {@link JobDto}
     * @return {@link Path} to a file of found FDs
     * @throws IOException problem writing to a file
     */
    private Path saveFoundFdsToResultFile(JobResult jobResult, List<_FunctionalDependency> fds, long jobId) throws IOException {

        jobResult.setNumFoundFd(fds.size());

        Path targetPathResult = getFdsResultFilePath(jobResult, jobId);

        Files.createDirectories(targetPathResult.getParent());
        Files.createFile(targetPathResult);

        FileWriter writer = new  FileWriter(targetPathResult.toFile());

        for (_FunctionalDependency fd : fds) {

            writer.write(fd.toString()+"\n");
        }
        writer.close();

        return targetPathResult;
    }

    /**
     * Send results of one job iteration to JobService.
     * Deletes a file of found FDs at the end.
     * @param jobResult {@link JobResult} job iteration data
     * @param foundFdsFilePath {@link Path} to a file of found FDs
     */
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

    /**
     * Update status of ONE JOB ITERATION -> JOB status is updated/computed automatically in JobService.
     * @param jobResultId {@link Long} id of job iteration
     * @param status new {@link JobStatus} of {@link JobResult}
     * @param serviceInstanceJob URI to JobService
     */
    private void updateStatus(Long jobResultId, JobStatus status, ServiceInstance serviceInstanceJob) {
        ResponseEntity<Void> response = restClient.patch()
                .uri(serviceInstanceJob.getUri() + "/jobs/iteration/" + jobResultId + "/status")
                .contentType(APPLICATION_JSON)
                .body(status)
                .retrieve()
                .toBodilessEntity();

    }

    /**
     * Get path to file storing found FDs in specified job iteration.
     * @param jobResult {@link JobResult} data
     * @param jobId {@link Long} id of Job
     * @return {@link Path} to file
     */
    private Path getFdsResultFilePath(JobResult jobResult, long jobId) {
        // fileName: job-ID-fdep-run-#-foundFDs.txt
        return Paths.get("datatmp/results/job-" + jobId
                + "-fdep-run-" + jobResult.getIteration() + "-foundFDs.txt");
    }

    /**
     * Generate unique Spark context group ID for job
     * @param jobId {@link Long}id of Job
     * @return unique {@link String} ID
     */
    private String getSparkContextGroupId(long jobId) {

        return "job"+jobId+"_"+ UUID.randomUUID();
    }
}
