package net.engio.pips.lab;

import net.engio.pips.lab.workload.ExecutionEvent;
import net.engio.pips.lab.workload.ITask;
import net.engio.pips.lab.workload.ITaskFactory;
import net.engio.pips.lab.workload.Workload;

import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Todo: Add javadoc
 *
 * @author bennidi
 *         Date: 6/19/14
 */
class WorkloadManager {

    private Workload workload;
    private Callable<Long> scheduler;
    private ExecutorService workloadExecutor;
    private List<Future> scheduledTasks = new LinkedList<Future>();
    private Future scheduledWorkload;
    private List<ExecutionContext> contexts = new LinkedList<ExecutionContext>();
    private volatile boolean stopped = false;

    WorkloadManager(Workload workload, Benchmark benchmark) {
        this.workload = workload;
        createScheduler(benchmark, benchmark.getClobalContext().getChild());
    }

    void stop() {
        stopped = true;
        for (Future task : scheduledTasks)
            task.cancel(true); // this doesn't seem to have any effect
        System.out.println("Canceling workload " + workload.getName());
        scheduledWorkload.cancel(true);
        workloadExecutor.shutdown();
    }

    Future start(ExecutorService executor) {
        return scheduledWorkload = executor.submit(scheduler);
    }

    List<ExecutionContext> getContexts() {
        return contexts;
    }

    // create a single executable unit which will run the tasks from the given workload
    // in its own thread pool
    private Callable<Long> createScheduler(final Benchmark benchmark, final ExecutionContext workloadContext) {
        workloadExecutor = Executors.newFixedThreadPool(workload.getParallelUnits(), new ThreadFactory() {

            private ThreadGroup group = new ThreadGroup(workload.getName());
            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(group, runnable, workload.getName());
                thread.setPriority(Thread.NORM_PRIORITY);
                return thread;
            }
        });
        scheduler = new Callable<Long>() {
            @Override
            public Long call() {
                final AtomicInteger scheduled = new AtomicInteger(0);// number of scheduled tasks
                final AtomicInteger finished = new AtomicInteger(0); // number of finished tasks
                //final ResultCollector collector = experiment.getResults();
                final ITaskFactory tasks = workload.getITaskFactory();
                final PrintWriter log = new PrintWriter(benchmark.getLogStream(), true);

                log.println("Starting workload " + workload);
                // call initialization handlers before scheduling the actual tasks
                workload.started();
                workload.getHandler(ExecutionEvent.WorkloadInitialization).handle(workloadContext);
                // create the tasks and schedule for execution
                for (int i = 0; i < workload.getParallelUnits(); i++) {
                    log.println("Scheduling task " + workload.getName() + "[" + scheduled.incrementAndGet() + "]");
                    final int taskNumber = i + 1;
                    final ExecutionContext taskContext = workloadContext.getChild();
                    contexts.add(taskContext);
                    // simply submit a runnable as return values are not important
                    // the runnable creates a new task and keeps executing it according to specified duration
                    scheduledTasks.add(workloadExecutor.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                ITask task = tasks.create(taskContext);
                                log.println("Executing task " + workload.getName() + "[" + taskNumber + "]");
                                int round = 0;
                                // execute number of times specified
                                if (workload.getDuration().isRepetitive()) {
                                    for (int i = 0; i < workload.getDuration().getRepetitions(); i++) {
                                        execute(task, workload, taskContext, log, taskNumber, ++round);
                                    }

                                } else { // or as long as depending task has not yet finished
                                    while (!stopped) {
                                        execute(task, workload, taskContext, log, taskNumber, ++round);
                                    }
                                }
                            } catch(InterruptedException e){
                                // this happens when the workload is shutdown
                                Thread.currentThread().interrupt();
                            } catch (Exception e) {
                                log.println("Task" + workload.getName() + "[" + taskNumber + "]" + "  threw an exception while orderly execution: " + e.toString());
                                e.printStackTrace();
                                //throw new RuntimeException(e);
                            } finally {
                                finished.incrementAndGet();
                                log.println("Finished task: " + workload.getName() + "[" + taskNumber + "]");
                                log.println("Tasks left in " + workload.getName() + ": " + (scheduled.get() - finished.get()));
                            }
                        }
                    }));
                }

                // wait until all tasks have been executed
                try {
                    while (scheduled.get() > finished.get())
                        Thread.sleep(1000);
                } catch (InterruptedException e) {
                    if (workload.getDuration().isDependent() && !workload.getDuration().getDependingOn().isFinished()) {
                        log.println(workload + " interrupted although dependent workload not finished");
                        e.printStackTrace(); // something was wrong here
                    }

                    if (!workload.getDuration().isTimeBased()
                            && !workload.getDuration().isDependent()) {
                        log.println(workload + " interrupted although no time based duration specified");
                        e.printStackTrace(); // something was wrong here
                    }
                    if (workload.getDuration().isTimeBased()
                            // interrupted before duration ends
                            && System.currentTimeMillis() < workload.getDuration().inMillisecs() + workload.getStarted()) {
                        log.println(workload + " interrupted before timer finished");
                        e.printStackTrace(); // something was wrong here
                    }

                } finally {
                    // signal end
                    workload.finished();
                    log.println("Finished workload: " + workload);
                    workload.getHandler(ExecutionEvent.WorkloadCompletion).handle(workloadContext);
                }
                return 1L;
            }

        };
        return scheduler;
    }


    private void execute(ITask task, Workload workload, ExecutionContext taskContext, PrintWriter log, int taskNumber, int round) throws InterruptedException {
        try {
            log.println(workload.getName() + "[" + taskNumber + "]->" + round);
            task.run(taskContext);
        } catch (Exception e) {
            log.println("Task" + workload.getName() + "[" + taskNumber + "]" + "  threw an exception while orderly execution: " + e.toString());
            e.printStackTrace();
            //throw new RuntimeException(e);
        }
        if (workload.hasDelay())
            Thread.sleep(workload.getDelay());
    }

}
