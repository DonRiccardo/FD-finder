package com.example.dataservice;

import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.server.RepresentationModelAssembler;
import org.springframework.stereotype.Component;

import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.linkTo;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.methodOn;

@Component
public class DatasetModelAssembler implements RepresentationModelAssembler<Dataset, EntityModel<Dataset>> {

    @Override
    public EntityModel<Dataset> toModel(Dataset dataset) {

        return EntityModel.of(dataset,
                linkTo(methodOn(DatasetController.class).one(dataset.getId())).withSelfRel(),
                linkTo(methodOn(DatasetController.class).all()).withRel("datasets"),
                linkTo(methodOn(DatasetController.class).delete(dataset.getId())).withRel("delete"),
                linkTo(methodOn(DatasetController.class).downloadFile(dataset.getId())).withRel("download"));
    }
}
