package edu.unc.mapseq.messaging.bergmips.alignment;

import java.io.IOException;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.WorkflowDAO;
import edu.unc.mapseq.dao.WorkflowRunAttemptDAO;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.dao.model.WorkflowRun;
import edu.unc.mapseq.dao.model.WorkflowRunAttempt;
import edu.unc.mapseq.dao.model.WorkflowRunAttemptStatusType;
import edu.unc.mapseq.workflow.WorkflowException;
import edu.unc.mapseq.workflow.model.WorkflowMessage;
import edu.unc.mapseq.workflow.sequencing.AbstractSequencingMessageListener;

public class BergMIPsAlignmentMessageListener extends AbstractSequencingMessageListener {

    private static final Logger logger = LoggerFactory.getLogger(BergMIPsAlignmentMessageListener.class);

    public BergMIPsAlignmentMessageListener() {
        super();
    }

    @Override
    public void onMessage(Message message) {
        logger.debug("ENTERING onMessage(Message)");

        String messageValue = null;

        try {
            if (message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;
                messageValue = textMessage.getText();
                logger.debug("Received TextMessage: {}", messageValue);
            }
        } catch (JMSException e2) {
            e2.printStackTrace();
        }

        if (StringUtils.isEmpty(messageValue)) {
            logger.warn("message value is empty");
            return;
        }

        logger.info("messageValue: {}", messageValue);

        ObjectMapper mapper = new ObjectMapper();
        WorkflowMessage workflowMessage = null;

        try {
            workflowMessage = mapper.readValue(messageValue, WorkflowMessage.class);
            if (workflowMessage.getEntities() == null) {
                logger.error("json lacks entities");
                return;
            }
        } catch (IOException e) {
            logger.error("BAD JSON format", e);
            return;
        }

        MaPSeqDAOBeanService daoBean = getWorkflowBeanService().getMaPSeqDAOBeanService();
        WorkflowDAO workflowDAO = daoBean.getWorkflowDAO();
        WorkflowRunAttemptDAO workflowRunAttemptDAO = daoBean.getWorkflowRunAttemptDAO();

        try {
            List<Workflow> workflowList = workflowDAO.findByName(getWorkflowName());
            if (CollectionUtils.isEmpty(workflowList)) {
                logger.error("No Workflow Found: {}", getWorkflowName());
                return;
            }
            Workflow workflow = workflowList.get(0);
            WorkflowRun workflowRun = createWorkflowRun(workflowMessage, workflow);

            WorkflowRunAttempt attempt = new WorkflowRunAttempt();
            attempt.setStatus(WorkflowRunAttemptStatusType.PENDING);
            attempt.setWorkflowRun(workflowRun);
            workflowRunAttemptDAO.save(attempt);

        } catch (WorkflowException | MaPSeqDAOException e1) {
            logger.error(e1.getMessage(), e1);
        }

    }

}
