package com.example.jobservice;

import org.springframework.hateoas.CollectionModel;
import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.IanaLinkRelations;
import org.springframework.hateoas.MediaTypes;
import org.springframework.hateoas.mediatype.problem.Problem;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.stream.Collectors;

import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.linkTo;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.methodOn;

@RestController
public class JobController {

    private final JobRepository jobRepository;
    private final JobModelAssembler jobAssembler;

    public JobController(JobRepository jobRepository,  JobModelAssembler jobAssembler) {
        this.jobRepository = jobRepository;
        this.jobAssembler = jobAssembler;
    }

    @PostMapping("/jobs")
    public ResponseEntity<?> newJob(@RequestBody Job job) {

        EntityModel<Job> jobEntity = jobAssembler.toModel(jobRepository.save(job));

        return ResponseEntity
                .created(jobEntity.getRequiredLink(IanaLinkRelations.SELF).toUri())
                .body(jobEntity);
    }

    @GetMapping("/jobs")
    public CollectionModel<EntityModel<Job>> all() {

        List<EntityModel<Job>> jobs = jobRepository.findAll()
                .stream()
                .map(jobAssembler::toModel)
                .collect(Collectors.toList());

        return CollectionModel.of(jobs, linkTo(methodOn(JobController.class).all()).withSelfRel());
    }

    @GetMapping("/jobs/{id}")
    public EntityModel<Job> one(@PathVariable Long id) {

        Job job = jobRepository.findById(id)
                .orElseThrow(() -> new JobNotFoundException(id));

        return jobAssembler.toModel(job);
    }

    @DeleteMapping("/jobs/{id}/cancel")
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

    @DeleteMapping("/jobs/{id}/delete")
    public ResponseEntity<?> delete(@PathVariable Long id) {
        Job job = jobRepository.findById(id)
                .orElseThrow(() -> new JobNotFoundException(id));

        if (job.isJobRunning()){
            // TODO
        }

        jobRepository.deleteById(id);
        return ResponseEntity.noContent().build();
    }

    @PutMapping("/jobs/{id}/start")
    public ResponseEntity<?> start(@PathVariable Long id) {

        Job job = jobRepository.findById(id)
                .orElseThrow(() -> new JobNotFoundException(id));

        if (job.isJobPossibleToRun()){
            job.setStatus(JobStatus.WAITING);
            // TODO
            return ResponseEntity.ok(jobAssembler.toModel(jobRepository.save(job)));
        }

        return ResponseEntity
                .status(HttpStatus.METHOD_NOT_ALLOWED)
                .header(HttpHeaders.CONTENT_TYPE, MediaTypes.HTTP_PROBLEM_DETAILS_JSON_VALUE)
                .body(Problem.create()
                        .withTitle("Method Not Allowed")
                        .withDetail("You are not allowed to start the job in state: "+job.getStatus().toString()));
    }
}
