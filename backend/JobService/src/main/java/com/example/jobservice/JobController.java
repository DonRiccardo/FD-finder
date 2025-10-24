package com.example.jobservice;

import com.example.jobservice.model.*;
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

    @PostMapping
    public ResponseEntity<?> newJob(@Valid @RequestBody Job job) {

        EntityModel<Job> jobEntity = jobAssembler.toModel(jobRepository.save(job));

        return ResponseEntity
                .created(jobEntity.getRequiredLink(IanaLinkRelations.SELF).toUri())
                .body(jobEntity);
    }

    @GetMapping
    public CollectionModel<EntityModel<Job>> all() {

        List<EntityModel<Job>> jobs = jobRepository.findAll()
                .stream()
                .map(jobAssembler::toModel)
                .collect(Collectors.toList());

        return CollectionModel.of(jobs, linkTo(methodOn(JobController.class).all()).withSelfRel());
    }

    @GetMapping("/{id}")
    public EntityModel<Job> one(@PathVariable Long id) {

        Job job = jobRepository.findById(id)
                .orElseThrow(() -> new JobNotFoundException(id));

        return jobAssembler.toModel(job);
    }

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

    @DeleteMapping("/{id}/delete")
    public ResponseEntity<?> delete(@PathVariable Long id) {

        Job job = jobRepository.findById(id)
                .orElseThrow(() -> new JobNotFoundException(id));
        Job newJob;
        if (job.isJobRunning()){

            for (String alg : job.getAlgorithm()) {
                try {

                    jobService.cancelJobAtAlgorithm(alg, job, discoveryClient, restClient);
                }
                catch (Exception e) {

                    job.setAlgorithmIterationStatus(alg, JobStatus.FAILED);
                    newJob = jobRepository.save(job);

                }

            }
        }

        jobRepository.deleteById(id);
        return ResponseEntity.noContent().build();
    }

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

    @GetMapping("/results/{id}")
    public EntityModel<JobResult> resultOfOneJobIteration(@PathVariable Long id){

        JobResult jobResult = jobResultsRepository.findById(id)
                .orElseThrow(() -> new ResultsNotFoundException(id));

        return jobResultsAssembler.toModel(jobResult);

    }

    @GetMapping("/{id}/results/graphdata")
    public ResponseEntity<?> resultsOfJobForGraph(@PathVariable Long id){
        // TODO endpoint na odoslanie averaged dát pre grafovu vizualizaciu
        // TODO ulozit spracovane data do JOB v get metode ak premenna==null
        // TODO object ako {graphMetrics: [], graphTime: []} - data pre graf s CPU+memory, data pre graf s casom

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

    @GetMapping("/{id}/results")
    public ResponseEntity<?> resultsOfJob(@PathVariable Long id) {
        // TODO vysledky JOBu cez vsetky iteracie algoritmov - priemer/max/min cez priemery iteracii

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

    @PatchMapping("/iteration/{id}/status")
    public ResponseEntity<?> updateStatus(@PathVariable Long id, @RequestBody JobStatus jobStatus) {
        JobResult jobResult = jobResultsRepository.findById(id)
                .orElseThrow(() -> new ResultsNotFoundException(id));

        if (jobResult.isJobRunning() && (jobStatus == JobStatus.RUNNING || jobStatus == JobStatus.DONE || jobStatus == JobStatus.FAILED)) {
            jobResult.setStatus(jobStatus);
            jobResultsRepository.save(jobResult);

            Job job = jobResult.getJob();
            if (job.setAndChangeJobStatusBasedOnIterations()) jobRepository.save(job);

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
