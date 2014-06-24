package net.engio.pips.lab;

import net.engio.pips.data.DataCollectorManager;
import net.engio.pips.data.IDataCollector;
import net.engio.pips.lab.workload.Workload;
import net.engio.pips.reports.IReporter;

import java.io.File;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.*;

/**
 * A benchmark is the container for all information of a formerly executed performance
 * measurement. It provides access to all collected data and means to create reports and
 * export them to persistent storage
 *
 * @author bennidi
 *         Date: 2/11/14
 */
public class Benchmark {

    public static final class Properties{
        public static final String TimeoutInSeconds = "Timeout in seconds";
        public static final String SampleInterval = "Sample interval";
        public static final String BasePath = "Base path";
        public static final String LogStream = "Log stream";
        public static final String Title = "Title";
        public static final String ReportBaseDir = "Report base dir";
    }

    private ExecutionContext rootContext = new ExecutionContext(this);

    private List<IReporter> reporters = new LinkedList<IReporter>();

    private List<Workload> workloads = new LinkedList<Workload>();

    private Executions executions;

    private String title;

    private DataCollectorManager collectors = new DataCollectorManager();

    public Benchmark(String title) {
        if (title == null || title.isEmpty())
            throw new IllegalArgumentException("Please provide a title that is a valid identifier for a directory");
        this.title = title;
    }

    public <V> IDataCollector<V> addCollector(IDataCollector<V> collector){
        return collectors.addCollector(collector);
    }

    public void setExecutions(Executions executions) {
        this.executions = executions;
    }

    public Executions getExecutions() {
        return executions;
    }

    public DataCollectorManager getCollectorManager(){
        return collectors;
    }

    public List<IDataCollector> getCollectors(){
        return getCollectors("");
    }

    public List<IDataCollector> getCollectors(String collectorId){
        return collectors.getCollectors(collectorId);
    }

    void verifyWorkloads(){
        // TOdo: check start/duration dependencies

        for(Workload workload : getWorkloads()){
            if(workload.getITaskFactory() == null)
                throw new LabException("Workload has no task factory:" + workload, LabException.ErrorCode.WLWithoutFactory);
            if(workload.getStartCondition() == null)
                throw new LabException("Workload has no start condition specified:" + workload, LabException.ErrorCode.WLWithoutStart);
            if(workload.getDuration() == null)
                throw new LabException("Workload has no duration specified:" + workload, LabException.ErrorCode.WLWithoutDuration);
        }

        // verify dependency graph
        for(Workload workload : getWorkloads()){
            // since there is always a finite number of workloads
            // traversing the links will either end in cycle or terminate with a workload that specifies an absolute start/duration
            if(!willStart(workload))
                throw new LabException("Cycle in workload start condition" + workload, LabException.ErrorCode.WLWithCycleInStart);
            if(!willEnd(workload))
                throw new LabException("Cycle in workload duration: " + workload, LabException.ErrorCode.WLWithCycleInDuration);
        }

    }


    private boolean willStart(Workload workload){
        if(workload.getStartCondition().isImmediately() || workload.getStartCondition().isTimebased())
            return true;
        Set<Workload> preceeding = new HashSet<Workload>();
        while(workload.getStartCondition().getPreceedingWorkload() != null){
            if(!preceeding.add(workload.getStartCondition().getPreceedingWorkload()))
                return false; // workload was reached before -> cycle
            else workload = workload.getStartCondition().getPreceedingWorkload();
        }
        return true;
    }

    private boolean willEnd(Workload workload){
        if(workload.getDuration().isRepetitive() || workload.getDuration().isTimeBased())
            return true;
        Set<Workload> preceeding = new HashSet<Workload>();
        while(workload.getDuration().getDependingOn() != null){
            if(!preceeding.add(workload.getDuration().getDependingOn()))
                return false;
            else workload = workload.getDuration().getDependingOn();
        }
        return true;
    }



    /**
     * Register a global object that can be accessed from the {@link ExecutionContext}
     *
     * @param key   - The identifier to be used for subsequent lookups using {@code get(key)}
     * @param value - The value to associate with the key
     * @return
     */
    public Benchmark setProperty(String key, Object value) {
        rootContext.bind(key, value);
        return this;
    }

    public Benchmark addWorkload(Workload... workload) {
        for (Workload wl : workload){
            if(wl.hasTasksToRun())workloads.add(wl);
            //else getLogStream()   // TODO: log warning
        }
        return this;
    }



    public Benchmark addReporter(IReporter reporter) {
        reporters.add(reporter);
        return this;
    }

    public void generateReports(IReporter ...reporters) throws Exception {
        PrintWriter log = new PrintWriter(getLogStream(), true);
        if (reporters.length == 0) {
            log.println("Skipping report generation because no reporters have been registered");
            return;
        }
        setProperty(Properties.ReportBaseDir, prepareDirectory());

        for (IReporter reporter : reporters) {
            log.println("Report" + reporter);
            reporter.generate(this);
        }
    }

    public String getReportBaseDir() {
        return getProperty(Properties.ReportBaseDir);
    }

    private String prepareDirectory() {
        //create directory
        File baseDir = new File(getProperty(Properties.BasePath) + File.separator + getTitle() + File.separator + System.currentTimeMillis());
        baseDir.mkdirs();
        return baseDir.getAbsolutePath() + File.separator;
    }

    public List<Workload> getWorkloads() {
        return workloads;
    }

    public boolean isDefined(String key) {
        return rootContext.containsKey(key);
    }

    public <T> T getProperty(String key) {
        return (T) rootContext.get(key);
    }

    public Benchmark setBasePath(String basePath) {
        return setProperty(Properties.BasePath, basePath);
    }

    public Benchmark setSampleInterval(int sampleInterval) {
        return setProperty(Properties.SampleInterval, sampleInterval);
    }

    public int getTimeoutInSeconds() {
        return getProperty(Properties.TimeoutInSeconds);
    }


    public OutputStream getLogStream() {
        return isDefined(Properties.LogStream) ? (OutputStream) getProperty(Properties.LogStream) : System.out;
    }

    public Benchmark setLogStream(OutputStream out) {
        return setProperty(Properties.LogStream, out);
    }

    @Override
    public String toString() {
        StringBuilder exp = new StringBuilder();
        exp.append("Experiment ");
        exp.append(title);
        exp.append(" with ");
        exp.append(workloads.size() + " workloads");
        exp.append("\n");

        for(Workload load : workloads){
            exp.append("\t");
            exp.append(load);
        }
        exp.append("\n");
        exp.append("and additional parameters:\n");
        for(Map.Entry entry : rootContext.getProperties().entrySet()){
            exp.append("\t");
            exp.append(entry.getKey());
            exp.append(":");
            exp.append(entry.getValue());
            exp.append("\n");
        }

        return exp.toString();
    }

    public String getTitle() {
        return title;
    }



    public ExecutionContext getClobalContext() {
        return rootContext;
    }
}

