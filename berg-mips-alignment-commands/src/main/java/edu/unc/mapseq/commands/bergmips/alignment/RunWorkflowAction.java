package edu.unc.mapseq.commands.bergmips.alignment;

import java.io.IOException;
import java.io.StringWriter;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Argument;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import edu.unc.mapseq.config.MaPSeqConfigurationService;
import edu.unc.mapseq.dao.MaPSeqDAOBeanService;

@Command(scope = "bergmips-alignment", name = "run-workflow", description = "Run Workflow")
@Service
public class RunWorkflowAction implements Action {

    @Argument(index = 0, name = "workflowRunName", description = "WorkflowRun.name", required = true, multiValued = false)
    private String workflowRunName;

    @Argument(index = 1, name = "sampleId", description = "sampleId", required = true, multiValued = false)
    private Long sampleId;

    @Reference
    private MaPSeqDAOBeanService maPSeqDAOBeanService;

    @Reference
    private MaPSeqConfigurationService maPSeqConfigurationService;

    public RunWorkflowAction() {
        super();
    }

    @Override
    public Object execute() {

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                String.format("nio://%s:61616", maPSeqConfigurationService.getWebServiceHost("localhost")));

        Connection connection = null;
        Session session = null;
        try {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("queue/gs.alignment");
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            StringWriter sw = new StringWriter();

            JsonGenerator generator = new JsonFactory().createGenerator(sw);

            generator.writeStartObject();
            generator.writeArrayFieldStart("entities");

            generator.writeStartObject();
            generator.writeStringField("entityType", "Sample");
            generator.writeStringField("id", sampleId.toString());
            generator.writeEndObject();

            generator.writeStartObject();
            generator.writeStringField("entityType", "WorkflowRun");
            generator.writeStringField("name", workflowRunName);
            generator.writeEndObject();

            generator.writeEndArray();
            generator.writeEndObject();

            generator.flush();
            generator.close();

            sw.flush();
            sw.close();

            producer.send(session.createTextMessage(sw.toString()));

        } catch (JMSException | IOException e) {
            e.printStackTrace();
        } finally {
            try {
                session.close();
                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }

        return null;
    }

    public Long getSampleId() {
        return sampleId;
    }

    public void setSampleId(Long sampleId) {
        this.sampleId = sampleId;
    }

    public String getWorkflowRunName() {
        return workflowRunName;
    }

    public void setWorkflowRunName(String workflowRunName) {
        this.workflowRunName = workflowRunName;
    }

}
