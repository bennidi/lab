package net.engio.pips.lab;

import net.engio.pips.lab.workload.ExecutionEvent;
import net.engio.pips.lab.workload.ExecutionHandler;
import net.engio.pips.lab.workload.Workload;

import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @Author bennidi
 */
public class Laboratory {

    public void run(Benchmark... benchmarks) throws Exception {
        for(Benchmark benchmark : benchmarks){
            benchmark.verifyWorkloads();
        }
        for(Benchmark benchmark : benchmarks){
            measure(benchmark);
            /*
            PrintWriter log = new PrintWriter(benchmark.getLogStream(), true);
            log.println("Generating reports....");
            benchmark.generateReports();   */
        }

        Thread.sleep(3000); // wait for shutdown
    }




    public void measure(final Benchmark benchmark) {
        // each workload will run in its own thread
        final ExecutorService executor = Executors.newFixedThreadPool(benchmark.getWorkloads().size(), new ThreadFactory() {

            private ThreadGroup group = new ThreadGroup("scheduler");

            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(group, runnable, "Workload scheduler");
                thread.setPriority(Thread.MIN_PRIORITY);
                return thread;
            }
        });

        // keeping track of workloads and their corresponding executables
        final Map<Workload, WorkloadManager> workloads = new HashMap<Workload, WorkloadManager>(benchmark.getWorkloads().size());
        //final Map<Workload, Future<Long>> scheduled = Collections.synchronizedMap(new HashMap<Workload, Future<Long>>(experiment.getWorkloads().size()));
        final AtomicInteger finishedWorkloads = new AtomicInteger(0);

        final PrintWriter log  = new PrintWriter(benchmark.getLogStream(), true);
        final Timer timer = new Timer(true);

        Date start = new Date(System.currentTimeMillis());
        log.println("Starting experiment at " + start );
        // prepare workloads
        for(final Workload workload : benchmark.getWorkloads()){
            workloads.put(workload, new WorkloadManager(workload, benchmark));

            // keep track of finished workloads
            workload.handle(ExecutionEvent.WorkloadCompletion, new ExecutionHandler() {
                @Override
                public void handle(ExecutionContext context) {
                    finishedWorkloads.incrementAndGet();
                }
            });

            // cancel workloads when duration is exceeded
            if(workload.getDuration().isTimeBased()){
                workload.handle(ExecutionEvent.WorkloadInitialization, new ExecutionHandler() {
                    @Override
                    public void handle(ExecutionContext context) {
                        Date timeout = new Date(System.currentTimeMillis() + workload.getDuration().inMillisecs());
                        log.println("Scheduling timertask to cancel " + workload + " in " + timeout);
                        timer.schedule(new TimerTask() {
                            @Override
                            public void run() {
                                workloads.get(workload).stop();
                            }
                        }, timeout);
                    }
                });
            }

            // wire up dependent workloads to be started when their predecessor completes
            if(workload.getStartCondition().isDependent()){
               workload.getStartCondition().getPreceedingWorkload().handle(ExecutionEvent.WorkloadCompletion, new ExecutionHandler() {
                   @Override
                   public void handle(ExecutionContext context) {
                       workloads.get(workload).start(executor);
                   }
               });
            }

            // wire up dependent workloads to be stopped when their predecessor completes
            if(workload.getDuration().isDependent()){
                workload.getDuration().getDependingOn().handle(ExecutionEvent.WorkloadCompletion, new ExecutionHandler() {
                    @Override
                    public void handle(ExecutionContext context) {
                        // interrupt the task
                        workloads.get(workload).stop();
                    }
                });
            }
        }

        // schedule workloads
        for(final Workload workload : benchmark.getWorkloads()){
            // either now
            if(workload.getStartCondition().isImmediately()){
                workloads.get(workload).start(executor);
            }
            // or in the future based on specified start condition
            else if(workload.getStartCondition().isTimebased()){
                timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        workloads.get(workload).start(executor);
                    }
                }, new Date(System.currentTimeMillis() + workload.getStartCondition().inMillisecs()));
            }
        }

        // wait until all tasks have been executed
        try {
            while(finishedWorkloads.get() < benchmark.getWorkloads().size())
                Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }finally {
            log.println("Finished experiment");

            // merge contexts
            Executions executions = new Executions();
            for(WorkloadManager workMan : workloads.values())
                 executions.addAll(workMan.getContexts());
            benchmark.setExecutions(executions);
        }

    }


}

