package edu.unc.mapseq.executor.bergmips.alignment;

import java.util.Date;
import java.util.List;
import java.util.TimerTask;

import org.apache.commons.collections.CollectionUtils;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.WorkflowDAO;
import edu.unc.mapseq.dao.WorkflowRunAttemptDAO;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.dao.model.WorkflowRunAttempt;
import edu.unc.mapseq.dao.model.WorkflowSystemType;
import edu.unc.mapseq.workflow.WorkflowBeanService;
import edu.unc.mapseq.workflow.WorkflowExecutor;
import edu.unc.mapseq.workflow.WorkflowTPE;
import edu.unc.mapseq.workflow.bergmips.alignment.BergMIPsAlignmentWorkflow;

public class BergMIPsAlignmentWorkflowExecutorTask extends TimerTask {

    private static final Logger logger = LoggerFactory.getLogger(BergMIPsAlignmentWorkflowExecutorTask.class);

    private static final WorkflowTPE threadPoolExecutor = new WorkflowTPE();

    private WorkflowBeanService workflowBeanService;

    private String workflowName;

    public BergMIPsAlignmentWorkflowExecutorTask() {
        super();
    }

    @Override
    public void run() {
        logger.info("ENTERING run()");

        threadPoolExecutor.setCorePoolSize(workflowBeanService.getCorePoolSize());
        threadPoolExecutor.setMaximumPoolSize(workflowBeanService.getMaxPoolSize());

        logger.info(String.format("ActiveCount: %d, TaskCount: %d, CompletedTaskCount: %d", threadPoolExecutor.getActiveCount(),
                threadPoolExecutor.getTaskCount(), threadPoolExecutor.getCompletedTaskCount()));

        MaPSeqDAOBeanService mapseqDAOBeanService = this.workflowBeanService.getMaPSeqDAOBeanService();
        WorkflowDAO workflowDAO = mapseqDAOBeanService.getWorkflowDAO();
        WorkflowRunAttemptDAO workflowRunAttemptDAO = mapseqDAOBeanService.getWorkflowRunAttemptDAO();

        try {

            Workflow workflow = null;
            List<Workflow> workflowList = workflowDAO.findByName(getWorkflowName());
            if (CollectionUtils.isEmpty(workflowList)) {
                workflow = new Workflow(getWorkflowName(), WorkflowSystemType.EXPERIMENTAL);
                workflow.setId(workflowDAO.save(workflow));
            } else {
                workflow = workflowList.get(0);
            }

            if (workflow == null) {
                logger.error("Could not find or create {} workflow", getWorkflowName());
                return;
            }

            BundleContext bundleContext = FrameworkUtil.getBundle(getClass()).getBundleContext();
            Bundle bundle = bundleContext.getBundle();
            String version = bundle.getVersion().toString();

            List<WorkflowRunAttempt> attempts = workflowRunAttemptDAO.findEnqueued(workflow.getId());
            if (CollectionUtils.isNotEmpty(attempts)) {
                logger.info("dequeuing {} WorkflowRunAttempt", attempts.size());
                for (WorkflowRunAttempt attempt : attempts) {

                    BergMIPsAlignmentWorkflow alignmentWorkflow = new BergMIPsAlignmentWorkflow();
                    attempt.setVersion(version);
                    attempt.setDequeued(new Date());
                    workflowRunAttemptDAO.save(attempt);

                    alignmentWorkflow.setWorkflowBeanService(workflowBeanService);
                    alignmentWorkflow.setWorkflowRunAttempt(attempt);
                    threadPoolExecutor.submit(new WorkflowExecutor(alignmentWorkflow));

                }

            }

        } catch (MaPSeqDAOException e) {
            e.printStackTrace();
        }

    }

    public String getWorkflowName() {
        return workflowName;
    }

    public void setWorkflowName(String workflowName) {
        this.workflowName = workflowName;
    }

    public WorkflowBeanService getWorkflowBeanService() {
        return workflowBeanService;
    }

    public void setWorkflowBeanService(WorkflowBeanService workflowBeanService) {
        this.workflowBeanService = workflowBeanService;
    }

}
