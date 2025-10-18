package com.example.jobservice.model;

import com.example.jobservice.JobController;
import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.server.RepresentationModelAssembler;
import org.springframework.stereotype.Component;

import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.linkTo;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.methodOn;

@Component
public class JobModelAssembler implements RepresentationModelAssembler<Job, EntityModel<Job>> {

    @Override
    public EntityModel<Job> toModel(Job job) {

        EntityModel<Job> entityModel = EntityModel.of(job,
                linkTo(methodOn(JobController.class).one(job.getId())).withSelfRel(),
                linkTo(methodOn(JobController.class).all()).withRel("jobs"));

        if (job.isJobRunning()) {

            entityModel.add(linkTo(methodOn(JobController.class).cancel(job.getId())).withRel("cancel"));
        } else if (job.isJobPossibleToRun()) {

            entityModel.add(linkTo(methodOn(JobController.class).start(job.getId())).withRel("start"));
            entityModel.add(linkTo(methodOn(JobController.class).delete(job.getId())).withRel("delete"));
        } else if (job.getStatus() == JobStatus.DONE) {
            entityModel.add(linkTo(methodOn(JobController.class).resultsOfJob(job.getId())).withRel("results"));
            entityModel.add(linkTo(methodOn(JobController.class).delete(job.getId())).withRel("delete"));
        }

        return entityModel;
    }
}
