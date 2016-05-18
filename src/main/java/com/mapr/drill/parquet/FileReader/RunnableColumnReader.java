package com.mapr.drill.parquet.FileReader;

/**
 * Created by pchandra on 5/5/16.
 * Reads (in one thread) an entire column, one page at a time.
 */
public class RunnableColumnReader implements Runnable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RunnableColumnReader.class);


  private boolean shutdown = false;

  public RunnableColumnReader() {
  }

  @Override public void run() {
    /*
    Thread.currentThread().setName("Workload:" + runner.getInstanceId());
    final Workload<BaseWorkloadConfig> workload = runner.getWorkload();
    try {
      while (workload.hasNext()) {
        if(shutdown || Thread.currentThread().isInterrupted()) {
          logger.info("Workload run interrupted by user.");
          break;
        }
        final WorkloadIteration iteration = workload.next();
        runner.run(iteration);
      }
      final WorkloadRunnerConfig config = runner.getConfig();
      config.getNotifier().onWorkloadComplete(runner, workload.getConfig(), config.getCollector().getWorkloadStats());
      runner.close();
    } catch (Exception e) {
      logger.info("Workload run interrupted by exception.");
    } finally {
    }
    */
  }

  public void shutdown(){
    this.shutdown=true;
  }


}
