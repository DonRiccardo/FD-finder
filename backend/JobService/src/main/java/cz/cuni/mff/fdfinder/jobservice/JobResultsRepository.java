package cz.cuni.mff.fdfinder.jobservice;

import cz.cuni.mff.fdfinder.jobservice.model.JobResult;
import org.springframework.data.jpa.repository.JpaRepository;

public interface JobResultsRepository extends JpaRepository<JobResult,Long> {
}
