package com.example.jobservice;

import jakarta.persistence.Entity;
import jakarta.validation.Valid;
import org.springframework.cloud.client.ServiceInstance;
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

    public JobController(JobService jobService , JobRepository jobRepository, JobResultsRepository jobResultsRepository,  JobModelAssembler jobAssembler, JobResultsModelAssembler jobResultsAssembler , DiscoveryClient discoveryClient, RestClient.Builder restClientBuilder) {
        this.jobService = jobService;
        this.jobRepository = jobRepository;
        this.jobResultsRepository = jobResultsRepository;
        this.jobAssembler = jobAssembler;
        this.jobResultsAssembler = jobResultsAssembler;
        this.discoveryClient = discoveryClient;
        this.restClient = restClientBuilder.build();
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

        if (job.isJobRunning()){
            job.setStatus(JobStatus.CANCELLED);
            // TODO
            return ResponseEntity.ok(jobAssembler.toModel(jobRepository.save(job)));
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

        if (job.isJobRunning()){
            // TODO
        }

        jobRepository.deleteById(id);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/{id}/start")
    public ResponseEntity<?> start(@PathVariable Long id) {

        Job job = jobRepository.findById(id)
                .orElseThrow(() -> new JobNotFoundException(id));

        if (job.isJobPossibleToRun()){
            job.setStatus(JobStatus.WAITING);

            ServiceInstance serviceInstance = discoveryClient.getInstances("algservice-" + job.getAlgorithm()).get(0);
            ResponseEntity<Void> response = restClient.post()
                    .uri(serviceInstance.getUri() + "/fdep/start/" + job.getId())
                    .accept(MediaType.APPLICATION_JSON)
                    .retrieve()
                    .toBodilessEntity();

            return ResponseEntity.ok(jobAssembler.toModel(jobRepository.save(job)));
        }

        return ResponseEntity
                .status(HttpStatus.METHOD_NOT_ALLOWED)
                .header(HttpHeaders.CONTENT_TYPE, MediaTypes.HTTP_PROBLEM_DETAILS_JSON_VALUE)
                .body(Problem.create()
                        .withTitle("Method Not Allowed")
                        .withDetail("You are not allowed to start the job in state: "+job.getStatus().toString()));
    }

    @PostMapping("/{id}/results")
    public ResponseEntity<?> results(
            @PathVariable Long id,
            @Valid @RequestPart("jobresult") JobResult jobResult,
            @RequestPart("file") MultipartFile file){

        jobResultsRepository.save(jobResult);
        jobService.uploadFile(id, file);

        return ResponseEntity
                .ok().build();
    }

    @GetMapping("/{id}/results")
    public EntityModel<JobResult> results(@PathVariable Long id){

        JobResult jobResult = jobResultsRepository.findById(id)
                .orElseThrow(() -> new ResultsNotFoundException(id));

        return jobResultsAssembler.toModel(jobResult);

    }

    @GetMapping("/{id}/results/fds")
    public ResponseEntity<?> resultsFds(@PathVariable Long id){
        try {
            Resource resource = jobService.getFile(id);

            return ResponseEntity
                    .ok()
                    .header(
                            HttpHeaders.CONTENT_DISPOSITION,
                            "attachment; filename=\"" + jobService.getResultsFileName(id) + "\""
                    )
                    .header(HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS, HttpHeaders.CONTENT_DISPOSITION)
                    .contentType(MediaType.parseMediaType("text/plain"))
                    .body(resource);

        }
        catch (IOException ex) {
            return ResponseEntity.internalServerError().build();
        }
    }

    @PatchMapping("/{id}/status")
    public ResponseEntity<?> updateStatus(@PathVariable Long id, @RequestBody JobStatus jobStatus) {
        Job job = jobRepository.findById(id)
                .orElseThrow(() -> new JobNotFoundException(id));

        if (job.isJobRunning() && (jobStatus == JobStatus.RUNNING || jobStatus == JobStatus.DONE || jobStatus == JobStatus.FAILED)) {
            job.setStatus(jobStatus);
            return ResponseEntity.ok(jobAssembler.toModel(jobRepository.save(job)));
        }

        return ResponseEntity
                .status(HttpStatus.METHOD_NOT_ALLOWED)
                .header(HttpHeaders.CONTENT_TYPE, MediaTypes.HTTP_PROBLEM_DETAILS_JSON_VALUE)
                .body(Problem.create()
                        .withTitle("Method Not Allowed")
                        .withDetail("You are not allowed to change the job status to: "+jobStatus.toString()));
    }
}
