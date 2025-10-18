package cz.cuni.mff.fdfinder.depminerservice.service;

import com.sun.management.OperatingSystemMXBean;
import cz.cuni.mff.fdfinder.depminerservice.model.Snapshot;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadMXBean;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;


@Service
public class JobMonitorService {

    private final OperatingSystemMXBean osBean =
            (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    private final MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();
    private final ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
    private ScheduledFuture<?> monitorTask;
    private final Map<Long, List<Snapshot>> jobStats = new ConcurrentHashMap<>();
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    public void startMonitoring(long jobIterationId) {
        if (monitorTask != null && !monitorTask.isCancelled()) {

            monitorTask.cancel(true);
        }

        jobStats.put(jobIterationId, new CopyOnWriteArrayList<>());

        monitorTask = executor.scheduleAtFixedRate(() -> {

            try {

                collectSnapshot(jobIterationId);
            } catch (Exception e) {

                e.printStackTrace();
            }
        }, 0, 500, TimeUnit.MILLISECONDS);
    }

    public void stopMonitoring(Long jobIterationId) {
        if (monitorTask != null && !monitorTask.isCancelled()) {

            monitorTask.cancel(true);
        }
    }

    private void collectSnapshot(long jobIterationId) {

        if (!jobStats.containsKey(jobIterationId)) return;

        long time = System.currentTimeMillis();
        long usedMemory = memBean.getHeapMemoryUsage().getUsed();
        double cpu = osBean.getCpuLoad();
        int threads = threadBean.getThreadCount();

        jobStats.get(jobIterationId).add(new Snapshot(time, usedMemory, cpu, threads));
    }

    public List<Snapshot> getSnapshots(long jobIterationId) {

        return jobStats.getOrDefault(jobIterationId, List.of());
    }

    public void deleteSnapshots(long jobIterationId) {

        jobStats.put(jobIterationId, new CopyOnWriteArrayList<>());
    }

    @PreDestroy
    public void destroy() {

        executor.shutdownNow();
    }

}


