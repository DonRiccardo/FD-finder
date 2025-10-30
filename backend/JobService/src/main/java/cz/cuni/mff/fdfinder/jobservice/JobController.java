package cz.cuni.mff.fdfinder.jobservice;

import cz.cuni.mff.fdfinder.jobservice.model.*;
import jakarta.validation.Valid;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.core.io.Resource;
import org.springframework.hateoas.CollectionModel;
import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.IanaLinkRelations;
import org.springframework.hateoas.MediaTypes;
import org.springframework.hateoas.mediatype.problem.Problem;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestClient;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.linkTo;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.methodOn;

/**
 * Controller of JobService. CrossOrigin exception for frontend.
 */
@CrossOrigin("http://localhost:5173")
@RestController
@RequestMapping("/jobs")
public class JobController {

    private final JobService jobService;
    private final JobRepository jobRepository;
    private final JobResultsRepository jobResultsRepository;
    private final JobModelAssembler jobAssembler;
    private final JobResultsModelAssembler jobResultsAssembler;
    private final DiscoveryClient discoveryClient;
    private final RestClient  restClient;
    private final SimpMessagingTemplate simpMessagingTemplate;

    public JobController(JobService jobService , JobRepository jobRepository, JobResultsRepository jobResultsRepository,
                         JobModelAssembler jobAssembler, JobResultsModelAssembler jobResultsAssembler ,
                         DiscoveryClient discoveryClient, RestClient.Builder restClientBuilder,
                         SimpMessagingTemplate simpMessagingTemplate) {
        this.jobService = jobService;
        this.jobRepository = jobRepository;
        this.jobResultsRepository = jobResultsRepository;
        this.jobAssembler = jobAssembler;
        this.jobResultsAssembler = jobResultsAssembler;
        this.discoveryClient = discoveryClient;
        this.restClient = restClientBuilder.build();
        this.simpMessagingTemplate = simpMessagingTemplate;
    }

    /**
     * Add a new job to DB.
     * @param job new job
     * @return HTTP CREATED
     */
    @PostMapping
    public ResponseEntity<?> newJob(@Valid @RequestBody Job job) {

        EntityModel<Job> jobEntity = jobAssembler.toModel(jobRepository.save(job));

        return ResponseEntity
                .created(jobEntity.getRequiredLink(IanaLinkRelations.SELF).toUri())
                .body(jobEntity);
    }

    /**
     * Get all jobs from DB.
     * @return jobs
     */
    @GetMapping
    public CollectionModel<EntityModel<Job>> all() {

        List<EntityModel<Job>> jobs = jobRepository.findAll()
                .stream()
                .map(jobAssembler::toModel)
                .collect(Collectors.toList());

        return CollectionModel.of(jobs, linkTo(methodOn(JobController.class).all()).withSelfRel());
    }

    /**
     * Get one job from DB with specified ID.
     * @param id id of the job.
     * @return Job; HTTP NOT FOUND if the specified Job cannot be found
     */
    @GetMapping("/{id}")
    public EntityModel<Job> one(@PathVariable Long id) {

        Job job = jobRepository.findById(id)
                .orElseThrow(() -> new JobNotFoundException(id));

        return jobAssembler.toModel(job);
    }

    /**
     * Cancel job if its running.
     * @param id id of the job to cancel
     * @return HTTP OK(Job); HTTP METHOD NOT ALLOWED if the job is not running;
     * HTTP NOT FOUND if the specified Job cannot be found
     */
    @DeleteMapping("/{id}/cancel")
    public ResponseEntity<?> cancel(@PathVariable Long id) {

        Job job = jobRepository.findById(id)
                .orElseThrow(() -> new JobNotFoundException(id));

        if (job.isJobRunning()) {
            job.setStatusToIterationsAndJob(JobStatus.CANCELLED);
            Job newJob = jobRepository.save(job);

            for (String alg : job.getAlgorithm()) {
                try {

                    jobService.cancelJobAtAlgorithm(alg, newJob, discoveryClient, restClient);
                }
                catch (Exception e) {

                    newJob.setAlgorithmIterationStatus(alg, JobStatus.FAILED);
                    newJob = jobRepository.save(newJob);
                }

            }

            return ResponseEntity.ok(jobAssembler.toModel(newJob));
        }

        return ResponseEntity
                .status(HttpStatus.METHOD_NOT_ALLOWED)
                .header(HttpHeaders.CONTENT_TYPE, MediaTypes.HTTP_PROBLEM_DETAILS_JSON_VALUE)
                .body(Problem.create()
                        .withTitle("Method Not Allowed")
                        .withDetail("You are not allowed to cancel the job in state: "+job.getStatus().toString()));
    }

    /**
     * Delete a job with specified ID.
     * @param id id of the job to delete
     * @return HTTP NO CONTENT; HTTP NOT FOUND if the specified Job cannot be found
     */
    @DeleteMapping("/{id}/delete")
    public ResponseEntity<?> delete(@PathVariable Long id) {

        Job job = jobRepository.findById(id)
                .orElseThrow(() -> new JobNotFoundException(id));

        if (job.isJobRunning()){

            for (String alg : job.getAlgorithm()) {
                try {

                    jobService.cancelJobAtAlgorithm(alg, job, discoveryClient, restClient);
                }
                catch (Exception e) {

                }

            }
        }

        jobRepository.deleteById(id);
        return ResponseEntity.noContent().build();
    }

    /**
     * Start a job with specified ID.
     * @param id id of the job to start
     * @return HTTP OK(Job); HTTP METHOD NOT ALLOWED if the job is not possible to run;
     * HTTP NOT FOUND if the specified Job Cannot be found
     */
    @PostMapping("/{id}/start")
    public ResponseEntity<?> start(@PathVariable Long id) {

        Job job = jobRepository.findById(id)
                .orElseThrow(() -> new JobNotFoundException(id));

        if (job.isJobPossibleToRun()){
            job.setStatusToIterationsAndJob(JobStatus.WAITING);
            Job newJob = jobRepository.save(job);

            for (String alg : job.getAlgorithm()) {
                try {

                    jobService.startJobAtAlgorithm(alg, newJob, discoveryClient, restClient);
                }
                catch (Exception e) {

                    newJob.setAlgorithmIterationStatus(alg, JobStatus.FAILED);
                    newJob = jobRepository.save(newJob);
                }

            }

            return ResponseEntity.ok(jobAssembler.toModel(newJob));
        }

        return ResponseEntity
                .status(HttpStatus.METHOD_NOT_ALLOWED)
                .header(HttpHeaders.CONTENT_TYPE, MediaTypes.HTTP_PROBLEM_DETAILS_JSON_VALUE)
                .body(Problem.create()
                        .withTitle("Method Not Allowed")
                        .withDetail("You are not allowed to start the job in state: "+job.getStatus().toString()));
    }

    /**
     * Update JobRsult and set with provided values anf file containing found FDs.
     * @param id id of the JobResult
     * @param jobResult new JobResult values
     * @param file file of founf FDs
     * @return HTTP OK; HTTP NOT FOUND if the specified JobResult cannot be found
     */
    @PatchMapping("/results/{id}")
    public ResponseEntity<?> postResultOfJobIteration(
            @PathVariable Long id,
            @Valid @RequestPart("jobresult") JobResult jobResult,
            @RequestPart("file") MultipartFile file){

        JobResult oldJobResult = jobResultsRepository.findById(id)
                .orElseThrow(() -> new ResultsNotFoundException(id));

        oldJobResult.setStatus(jobResult.getStatus());
        oldJobResult.setStartTime(jobResult.getStartTime());
        oldJobResult.setEndTime(jobResult.getEndTime());
        oldJobResult.setNumFoundFd(jobResult.getNumFoundFd());
        oldJobResult.setSnapshots(jobResult.getSnapshots());

        jobResultsRepository.save(oldJobResult);
        jobService.uploadFile(oldJobResult, file);

        return ResponseEntity
                .ok().build();
    }

    /**
     * Get the results of a specified JobIteration.
     * @param id id of the JobResult
     * @return JobResult; HTTP NOT FOUND if the specified JobResult cannot be found
     */
    @GetMapping("/results/{id}")
    public EntityModel<JobResult> resultOfOneJobIteration(@PathVariable Long id){

        JobResult jobResult = jobResultsRepository.findById(id)
                .orElseThrow(() -> new ResultsNotFoundException(id));

        return jobResultsAssembler.toModel(jobResult);

    }

    /**
     * Get data prepared to graph visualization.
     * @param id id of the Job
     * @return HTTP OK(data for graphs); HTTP METHOD NOT ALLOWED if the Job is not DONE
     */
    @GetMapping("/{id}/results/graphdata")
    public ResponseEntity<?> resultsOfJobForGraph(@PathVariable Long id){

        Job job = jobRepository.findById(id)
                .orElseThrow(() -> new JobNotFoundException(id));

        if (job.getStatus() != JobStatus.DONE){

            return ResponseEntity
                    .status(HttpStatus.METHOD_NOT_ALLOWED)
                    .header(HttpHeaders.CONTENT_TYPE, MediaTypes.HTTP_PROBLEM_DETAILS_JSON_VALUE)
                    .body(Problem.create()
                            .withTitle("Method Not Allowed")
                            .withDetail("You are not allowed to obtain data from unfinished job."));
        }

        return ResponseEntity.ok(EntityModel.of(jobService.prepareSnapshotDataForVisualization(job)));
    }

    /**
     * Get a result of one Job.
     * @param id id of the Job
     * @return HTTP OK(Job); HTTP NOT FOUND if the specified Job cannot be found
     */
    @GetMapping("/{id}/results")
    public ResponseEntity<?> resultsOfJob(@PathVariable Long id) {

        Job job = jobRepository.findById(id)
                .orElseThrow(() -> new JobNotFoundException(id));

        if (job.getStatus() != JobStatus.DONE){

            return ResponseEntity
                    .status(HttpStatus.METHOD_NOT_ALLOWED)
                    .header(HttpHeaders.CONTENT_TYPE, MediaTypes.HTTP_PROBLEM_DETAILS_JSON_VALUE)
                    .body(Problem.create()
                            .withTitle("Method Not Allowed")
                            .withDetail("You are not allowed to obtain data from unfinished job."));
        }

        return ResponseEntity.ok(EntityModel.of(jobService.prepareStatisticForJob(job)));

    }

    /**
     * Get the file containing founf FDs.
     * @param id id of the JobResult
     * @return HTTP OK (file of found FDs); HTTP NOT FOUND if specified JobResult cannot be found
     */
    @GetMapping("/results/{id}/fds")
    public ResponseEntity<?> resultsFds(@PathVariable Long id){
        try {
            JobResult jobResult = jobResultsRepository.findById(id)
                    .orElseThrow(() -> new ResultsNotFoundException(id));

            Resource resource = jobService.getFile(jobResult);

            return ResponseEntity
                    .ok()
                    .header(
                            HttpHeaders.CONTENT_DISPOSITION,
                            "attachment; filename=\"" + jobService.getResultsFileName(jobResult) + "\""
                    )
                    .header(HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS, HttpHeaders.CONTENT_DISPOSITION)
                    .contentType(MediaType.parseMediaType("text/plain"))
                    .body(resource);

        }
        catch (IOException ex) {
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Update status of specified JobResult.
     * @param id id of hte JobResult
     * @param jobStatus new JobResult status
     * @return HTTP OK; HTTP NOT FOUND if the specified JobResult cannot be found; HTTP METHOD NOT ALLOWED
     * if the new status cannot be set
     */
    @PatchMapping("/iteration/{id}/status")
    public ResponseEntity<?> updateStatus(@PathVariable Long id, @RequestBody JobStatus jobStatus) {
        JobResult jobResult = jobResultsRepository.findById(id)
                .orElseThrow(() -> new ResultsNotFoundException(id));

        if (jobResult.isJobIterationRunning() && (jobStatus == JobStatus.RUNNING || jobStatus == JobStatus.DONE || jobStatus == JobStatus.FAILED)) {
            jobResult.setStatus(jobStatus);
            jobResultsRepository.save(jobResult);

            Job job = jobResult.getJob();
            if (job.setAndChangeJobStatusBasedOnIterations()) {
                if (job.getStatus() == JobStatus.FAILED) {
                    for (String alg : job.getAlgorithm()) {
                        try {

                            jobService.cancelJobAtAlgorithm(alg, job, discoveryClient, restClient);
                        }
                        catch (Exception e) {

                        }

                    }
                }
                jobRepository.save(job);
            }

            simpMessagingTemplate.convertAndSend("/topic/jobs", jobAssembler.toModel(job));

            return ResponseEntity.ok(jobResultsAssembler.toModel(jobResult));
        }


        return ResponseEntity
                .status(HttpStatus.METHOD_NOT_ALLOWED)
                .header(HttpHeaders.CONTENT_TYPE, MediaTypes.HTTP_PROBLEM_DETAILS_JSON_VALUE)
                .body(Problem.create()
                        .withTitle("Method Not Allowed")
                        .withDetail("You are not allowed to change the job status to: "+jobStatus.toString() + " from:" + jobResult.getStatus().toString()));
    }
}
