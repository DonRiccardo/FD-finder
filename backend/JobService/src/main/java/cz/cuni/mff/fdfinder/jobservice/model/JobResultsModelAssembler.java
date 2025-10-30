package cz.cuni.mff.fdfinder.jobservice.model;

import cz.cuni.mff.fdfinder.jobservice.JobController;
import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.server.RepresentationModelAssembler;
import org.springframework.stereotype.Component;

import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.linkTo;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.methodOn;

/**
 * Converts {@link JobResult} to {@link EntityModel} with links.
 */
@Component
public class JobResultsModelAssembler implements RepresentationModelAssembler<JobResult, EntityModel<JobResult>> {

    /**
     * Creates {@link EntityModel} of specified {@link JobResult} with links to other endpoints.
     * @param jobResult {@link JobResult} of which create Entity
     * @return {@link EntityModel} of the specified {@link JobResult}
     */
    @Override
    public EntityModel<JobResult> toModel(JobResult jobResult) {

        EntityModel<JobResult> entityModel = EntityModel.of(jobResult,
                linkTo(methodOn(JobController.class).resultOfOneJobIteration(jobResult.getId())).withSelfRel(),
                linkTo(methodOn(JobController.class).all()).withRel("jobs"),
                linkTo(methodOn(JobController.class).resultsFds(jobResult.getJob().getId())).withRel("resultsFds"));


        return entityModel;
    }
}
