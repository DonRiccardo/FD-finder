package com.example.jobservice;

import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.server.RepresentationModelAssembler;
import org.springframework.stereotype.Component;

import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.linkTo;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.methodOn;

@Component
public class JobResultsModelAssembler implements RepresentationModelAssembler<JobResult, EntityModel<JobResult>> {

    @Override
    public EntityModel<JobResult> toModel(JobResult jobResult) {

        EntityModel<JobResult> entityModel = EntityModel.of(jobResult,
                linkTo(methodOn(JobController.class).results(jobResult.getJobId())).withSelfRel(),
                linkTo(methodOn(JobController.class).all()).withRel("jobs"),
                linkTo(methodOn(JobController.class).resultsFds(jobResult.getJobId())).withRel("resultsFds"));


        return entityModel;
    }
}
