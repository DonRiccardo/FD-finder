package com.example.dataservice;

import jakarta.validation.Valid;
import org.springframework.hateoas.CollectionModel;
import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.IanaLinkRelations;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.stream.Collectors;

import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.linkTo;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.methodOn;

@RestController
public class DatasetController {

    private final DatasetRepository datasetRepository;
    private final DatasetModelAssembler datasetAssembler;

    public DatasetController(DatasetRepository datasetRepository,  DatasetModelAssembler datasetAssembler) {

        this.datasetRepository = datasetRepository;
        this.datasetAssembler = datasetAssembler;
    }

    @PostMapping("/datasets")
    public ResponseEntity<?> newDataset(@Valid @RequestBody Dataset dataset) {

        EntityModel<Dataset> entityModel = datasetAssembler.toModel(datasetRepository.save(dataset));

        return ResponseEntity
                .created(entityModel.getRequiredLink(IanaLinkRelations.SELF).toUri())
                .body(entityModel);
    }

    @GetMapping("/datasets")
    public CollectionModel<EntityModel<Dataset>> all() {

        List<EntityModel<Dataset>> datasets = datasetRepository.findAll().stream()
                .map(datasetAssembler::toModel)
                .collect(Collectors.toList());

        return CollectionModel.of(datasets, linkTo(methodOn(DatasetController.class).all()).withSelfRel());
    }

    @GetMapping("/datasets/{id}")
    public EntityModel<Dataset> one(@PathVariable Long id) {

        Dataset dataset = datasetRepository.findById(id)
                .orElseThrow(() -> new DatasetNotFoundException(id));

        return datasetAssembler.toModel(dataset);
    }

    @DeleteMapping("/datasets/{id}/delete")
    public ResponseEntity<?> delete(@PathVariable Long id) {

        datasetRepository.deleteById(id);

        return ResponseEntity.noContent().build();
    }
}
