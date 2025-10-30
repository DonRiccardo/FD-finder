package cz.cuni.mff.fdfinder.dataservice;

import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.server.RepresentationModelAssembler;
import org.springframework.stereotype.Component;

import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.linkTo;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.methodOn;

/**
 * Converts {@link Dataset} to {@link EntityModel} with links.
 */
@Component
public class DatasetModelAssembler implements RepresentationModelAssembler<Dataset, EntityModel<Dataset>> {

    /**
     * Creates {@link EntityModel} of specified {@link Dataset} with links to other endpoints and actions possible to do with the {@link Dataset}.
     * @param dataset {@link Dataset} of which create Entity
     * @return {@link EntityModel} of the specified {@link Dataset}
     */
    @Override
    public EntityModel<Dataset> toModel(Dataset dataset) {

        return EntityModel.of(dataset,
                linkTo(methodOn(DatasetController.class).one(dataset.getId())).withSelfRel(),
                linkTo(methodOn(DatasetController.class).all()).withRel("datasets"),
                linkTo(methodOn(DatasetController.class).delete(dataset.getId())).withRel("delete"),
                linkTo(methodOn(DatasetController.class).downloadFile(dataset.getId())).withRel("download"));
    }
}
