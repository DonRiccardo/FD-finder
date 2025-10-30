package cz.cuni.mff.fdfinder.jobservice;

import cz.cuni.mff.fdfinder.jobservice.model.Job;
import org.springframework.data.jpa.repository.JpaRepository;

public interface JobRepository extends JpaRepository<Job, Long> {
}
