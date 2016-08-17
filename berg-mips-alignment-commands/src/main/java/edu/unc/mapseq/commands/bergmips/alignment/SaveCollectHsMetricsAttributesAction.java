package edu.unc.mapseq.commands.bergmips.alignment;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.Option;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.commons.bergmips.alignment.SaveCollectHsMetricsAttributesRunnable;
import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.WorkflowRun;

@Command(scope = "bergmips-alignment", name = "save-collect-hs-metrics-attributes", description = "Save CollectHsMetrics Attributes")
@Service
public class SaveCollectHsMetricsAttributesAction implements Action {

    private final Logger logger = LoggerFactory.getLogger(SaveCollectHsMetricsAttributesAction.class);

    @Option(name = "--sampleId", description = "Sample Identifier", required = false, multiValued = false)
    private Long sampleId;

    @Option(name = "--flowcellId", description = "Flowcell Identifier", required = false, multiValued = false)
    private Long flowcellId;

    @Option(name = "--workflowRunId", description = "WorkflowRun Identifier", required = true, multiValued = false)
    private Long workflowRunId;

    @Reference
    private MaPSeqDAOBeanService maPSeqDAOBeanService;

    @Override
    public Object execute() {
        logger.debug("ENTERING execute()");

        if (sampleId == null && flowcellId == null) {
            System.out.println("Both the Sample & Flowcell identifiers can't be null");
            return null;
        }

        try {
            ExecutorService es = Executors.newSingleThreadExecutor();
            WorkflowRun workflowRun = maPSeqDAOBeanService.getWorkflowRunDAO().findById(workflowRunId);

            SaveCollectHsMetricsAttributesRunnable runnable = new SaveCollectHsMetricsAttributesRunnable(maPSeqDAOBeanService, workflowRun);
            if (sampleId != null) {
                runnable.setSampleId(sampleId);
            }
            if (flowcellId != null) {
                runnable.setFlowcellId(flowcellId);
            }
            es.submit(runnable);
            es.shutdown();
        } catch (MaPSeqDAOException e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    public Long getSampleId() {
        return sampleId;
    }

    public void setSampleId(Long sampleId) {
        this.sampleId = sampleId;
    }

    public Long getFlowcellId() {
        return flowcellId;
    }

    public void setFlowcellId(Long flowcellId) {
        this.flowcellId = flowcellId;
    }
}
