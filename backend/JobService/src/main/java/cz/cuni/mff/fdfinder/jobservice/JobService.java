package cz.cuni.mff.fdfinder.jobservice;

import cz.cuni.mff.fdfinder.jobservice.model.*;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import org.springframework.web.client.RestClient;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Service doing preparation and get work for Job.
 */
@Service
public class JobService {

    private final String jobResultsDirectory = "jobs/results/";
    /**
     * Size of a bin for sorting snapshots based on their time.
     */
    private final long binSizeMiliseconds = 500L;

    /**
     * Save file of found FDs for specified JobResult.
     * @param jobResult {@link JobResult}
     * @param file file of found FDs
     */
    public void uploadFile(JobResult jobResult, MultipartFile file) {

        final String filename = getResultsFileName(jobResult);

        saveFileData(file, getResultsFilePath(filename));
    }

    /**
     * Get a path for the file of found FDs.
     * @param fileName prepared file name
     * @return path to the file of found FDs.
     */
    private Path getResultsFilePath(String fileName) {

        return Paths.get(this.jobResultsDirectory, fileName).toAbsolutePath().normalize();
    }

    /**
     * Get name for the file of found FDs
     * @param jobResult {@link JobResult}
     * @return prepared file name in specified format
     */
    public String getResultsFileName(JobResult jobResult) {
        // fileName in format: job-ID-ALGname-run-#-foundFDs.txt
        return "job-" + jobResult.getJob().getId() + "-"
                + jobResult.getAlgorithm() + "-run-"
                + jobResult.getIteration() + "-foundFDs.txt";
    }

    /**
     * Save file in the specified path.
     * @param file file to save
     * @param path path where the file will be stored
     */
    private void saveFileData(MultipartFile file, Path path) {
        try {
            Files.createDirectories(path.getParent());
            file.transferTo(path.toFile());
        } catch (Exception e) {
            throw new RuntimeException("Failed to save dataset file", e);
        }
    }

    /**
     * Get (for download) {@link Resource} to the file of found FDs for specified JobResult.
     * @param jobResult {@link JobResult}
     * @return resource to the file
     * @throws IOException if the URL is malformed
     */
    public Resource getFile(JobResult jobResult) throws IOException {
        Path path = getResultsFilePath(getResultsFileName(jobResult));

        if(!Files.exists(path)) {
            throw new ResultsNotFoundException(jobResult.getJob().getId());
        }

        return new UrlResource(path.toUri());

    }

    /**
     * Send request to the specified ALG to start specified Job.
     * @param alg algorithm to start
     * @param job {@link Job} data
     * @param discoveryClient to get URI of ALG service
     * @param restClient to create request
     */
    public void startJobAtAlgorithm(String alg, Job job, DiscoveryClient discoveryClient, RestClient  restClient) {

        ServiceInstance serviceInstance = discoveryClient.getInstances("algservice-" + alg).getFirst();
        ResponseEntity<Void> response = restClient.post()
                .uri(serviceInstance.getUri() + "/" + alg + "/start/" + job.getId())
                .contentType(MediaType.APPLICATION_JSON)
                .body(job)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .toBodilessEntity();
    }

    /**
     * Cancel (stop running) a Job on specified algorithm.
     * @param alg algorithm on which the job is running
     * @param job {@link Job} you want to cancel
     * @param discoveryClient to get URI of ALG service
     * @param restClient to create request
     */
    public void cancelJobAtAlgorithm(String alg, Job job, DiscoveryClient discoveryClient, RestClient restClient) {

        ServiceInstance serviceInstance = discoveryClient.getInstances("algservice-" + alg).getFirst();
        ResponseEntity<Void> response = restClient.post()
                .uri(serviceInstance.getUri() + "/" + alg + "/cancel/" + job.getId())
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .toBodilessEntity();
    }

    /**
     * Get snapshots for specified Job.
     * @param job {@link Job}
     * @return Map of List({@link Snapshot}) for each algorithm separately
     */
    private Map<String, List<Snapshot>> getSnapshotsPerAlgorithm(Job job){

        Map<String, List<Snapshot>> snapshotsPerAlgorithm = new HashMap<>();

        for (JobResult jobResult : job.getJobResults()) {

            snapshotsPerAlgorithm.computeIfAbsent(jobResult.getAlgorithm(), k -> new ArrayList<>())
                    .addAll(jobResult.getSnapshots());
        }

        return snapshotsPerAlgorithm;
    }

    /**
     * Get JobResults for specified Job.
     * @param job {@link Job}
     * @return Map of List({@link JobResult}) for each algorithm separately
     */
    private Map<String, List<JobResult>> getJobResultsPerAlgorithm(Job job){

        Map<String, List<JobResult>> jobResultsPerAlgorithm = new HashMap<>();

        for (JobResult jobResult : job.getJobResults()) {

            jobResultsPerAlgorithm.computeIfAbsent(jobResult.getAlgorithm(), k -> new ArrayList<>()).add(jobResult);
        }

        return jobResultsPerAlgorithm;
    }

    /**
     * Get and prepare data for graph visualization for specified Job.
     * @param job {@link Job}
     * @return Map of List({@link MetricPoint}) for each algorithm separately
     */
    public Map<String, List<MetricPoint>> prepareSnapshotDataForVisualization(Job job) {

        Map<String, List<Snapshot>> snapshotsPerAlgorithm = getSnapshotsPerAlgorithm(job);

        Map<String, Map<Long, List<Snapshot>>> snapshotsPerAlgorithmPerTimeBin = new HashMap<>();

        for (Map.Entry<String, List<Snapshot>> entry : snapshotsPerAlgorithm.entrySet()) {

            Map<Long, List<Snapshot>> bins = new HashMap<>();

            for (Snapshot snapshot : entry.getValue()) {

                long bin = snapshot.getTimestamp() / binSizeMiliseconds;
                bins.computeIfAbsent(bin, k -> new ArrayList<>()).add(snapshot);
            }

            snapshotsPerAlgorithmPerTimeBin.put(entry.getKey(), bins);
        }

        Map<String, List<MetricPoint>> averagedMetrics = new HashMap<>();

        for (Map.Entry<String, Map<Long, List<Snapshot>>> entry : snapshotsPerAlgorithmPerTimeBin.entrySet()) {

            Map<Long, List<Snapshot>> bins = entry.getValue();

            List<MetricPoint> points = new ArrayList<>();
            for (Map.Entry<Long, List<Snapshot>> binEntry : bins.entrySet()) {

                long binTime = binEntry.getKey() * binSizeMiliseconds;
                List<Snapshot> binSnapshots = binEntry.getValue();

                double avgCpu = binSnapshots.stream().mapToDouble(Snapshot::getCpuLoad).average().orElse(0);
                avgCpu = avgCpu * 100;
                long avgMemory = (long) binSnapshots.stream().mapToLong(Snapshot::getUsedMemory).average().orElse(0);
                avgMemory = avgMemory / (1024 * 1024);

                points.add(new MetricPoint(binTime, avgCpu, avgMemory));
            }

            points.sort(Comparator.comparingLong(MetricPoint::getTimestamp));
            averagedMetrics.put(entry.getKey(), points);
        }

        return averagedMetrics;

    }

    /**
     * Get and prepare statistics for specified {@link Job}.
     * Statistics contain Min, Avg, Max value for CPU usage, memory usage and time consumption.
     * @param job {@link Job}
     * @return Map of {@link JobStatistics} for each algorithm separately
     */
    public Map<String, JobStatistics> prepareStatisticForJob(Job job) {

        Map<String, List<Snapshot>> snapshotsPerAlgorithm = getSnapshotsPerAlgorithm(job);
        Map<String, List<JobResult>> jobResultsPerAlgorithm = getJobResultsPerAlgorithm(job);
        Map<String, JobStatistics> processedMetrics = new HashMap<>();

        for (Map.Entry<String, List<Snapshot>> entry : snapshotsPerAlgorithm.entrySet()) {

            JobStatistics jobStats = getStatisticsForAlgorithm(entry.getValue());
            processedMetrics.put(entry.getKey(), jobStats);
        }

        for (Map.Entry<String, List<JobResult>> entry : jobResultsPerAlgorithm.entrySet()) {

            long timeSum = 0;
            int fdsSum = 0;

            List<JobResult> jobResults = entry.getValue();
            processedMetrics.get(entry.getKey()).setTimeMin(jobResults.getFirst().getDuration());
            processedMetrics.get(entry.getKey()).setFdsMin(jobResults.getFirst().getNumFoundFd());

            for (JobResult jobResult : jobResults) {

                long time = jobResult.getDuration();
                int fds =  jobResult.getNumFoundFd();
                timeSum += time;
                fdsSum += fds;

                if (time > processedMetrics.get(entry.getKey()).getTimeMax()) { processedMetrics.get(entry.getKey()).setTimeMax(time); }
                if (time < processedMetrics.get(entry.getKey()).getTimeMin()) { processedMetrics.get(entry.getKey()).setTimeMin(time); }

                if (fds > processedMetrics.get(entry.getKey()).getFdsMax()) { processedMetrics.get(entry.getKey()).setFdsMax(fds); }
                if (fds < processedMetrics.get(entry.getKey()).getFdsMin()) { processedMetrics.get(entry.getKey()).setFdsMin(fds); }
            }

            processedMetrics.get(entry.getKey()).setTimeAvg((double) timeSum / jobResults.size());
            processedMetrics.get(entry.getKey()).setFdsAvg(fdsSum / jobResults.size());
        }



        return processedMetrics;
    }

    /**
     * Get Statistic from provided {@link Snapshot}s.
     * Calculate Min, Avg, Max value for CPU usage, memory usage and time consumption.
     * @param snapshots List of {@link Snapshot}
     * @return {@link JobStatistics} object
     */
    private JobStatistics getStatisticsForAlgorithm(List<Snapshot> snapshots) {

        JobStatistics jobStats = new JobStatistics();

        try {
            double cpuSum = 0;
            long memorySum = 0;

            Snapshot first = snapshots.getFirst();
            jobStats.setCpuMin(first.getCpuLoad());
            jobStats.setCpuMax(first.getCpuLoad());
            jobStats.setMemoryMin(first.getUsedMemory());
            jobStats.setMemoryMax(first.getUsedMemory());

            for (Snapshot snapshot : snapshots) {

                double cpuLoad = snapshot.getCpuLoad();
                long memory = snapshot.getUsedMemory();

                cpuSum += cpuLoad;
                memorySum += memory;

                if (cpuLoad > jobStats.getCpuMax()){ jobStats.setCpuMax(cpuLoad); }
                if (cpuLoad < jobStats.getCpuMin()){ jobStats.setCpuMin(cpuLoad); }

                if (memory > jobStats.getMemoryMax()){ jobStats.setMemoryMax(memory); }
                if (memory < jobStats.getMemoryMin()){ jobStats.setMemoryMin(memory); }
            }

            int n = snapshots.size();
            jobStats.setCpuAvg(cpuSum / n);
            jobStats.setMemoryAvg((double) memorySum / n);
        }
        catch (NoSuchElementException e) {


        }



        return jobStats;
    }
}
