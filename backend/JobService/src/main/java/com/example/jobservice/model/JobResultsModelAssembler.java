package com.example.jobservice.model;

import com.example.jobservice.JobController;
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
                linkTo(methodOn(JobController.class).resultOfOneJobIteration(jobResult.getId())).withSelfRel(),
                linkTo(methodOn(JobController.class).all()).withRel("jobs"),
                linkTo(methodOn(JobController.class).resultsFds(jobResult.getJob().getId())).withRel("resultsFds"));


        return entityModel;
    }
}
