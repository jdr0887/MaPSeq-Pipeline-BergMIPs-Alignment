package edu.unc.mapseq.workflow;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.junit.Test;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobBuilder;
import org.renci.jlrm.condor.CondorJobEdge;
import org.renci.jlrm.condor.ext.CondorDOTExporter;
import org.renci.jlrm.condor.ext.CondorJobVertexNameProvider;

import edu.unc.mapseq.module.sequencing.WriteVCFHeaderCLI;
import edu.unc.mapseq.module.sequencing.bwa.BWAAlignCLI;
import edu.unc.mapseq.module.sequencing.bwa.BWASAMPairedEndCLI;
import edu.unc.mapseq.module.sequencing.fastqc.FastQCCLI;
import edu.unc.mapseq.module.sequencing.filter.FilterVariantCLI;
import edu.unc.mapseq.module.sequencing.gatk.GATKApplyRecalibrationCLI;
import edu.unc.mapseq.module.sequencing.gatk.GATKCountCovariatesCLI;
import edu.unc.mapseq.module.sequencing.gatk.GATKDepthOfCoverageCLI;
import edu.unc.mapseq.module.sequencing.gatk.GATKFlagStatCLI;
import edu.unc.mapseq.module.sequencing.gatk.GATKIndelRealignerCLI;
import edu.unc.mapseq.module.sequencing.gatk.GATKRealignerTargetCreatorCLI;
import edu.unc.mapseq.module.sequencing.gatk.GATKTableRecalibrationCLI;
import edu.unc.mapseq.module.sequencing.gatk.GATKUnifiedGenotyperCLI;
import edu.unc.mapseq.module.sequencing.gatk.GATKVariantRecalibratorCLI;
import edu.unc.mapseq.module.sequencing.picard.PicardAddOrReplaceReadGroupsCLI;
import edu.unc.mapseq.module.sequencing.picard.PicardFixMateCLI;
import edu.unc.mapseq.module.sequencing.picard.PicardMarkDuplicatesCLI;
import edu.unc.mapseq.module.sequencing.samtools.SAMToolsFlagstatCLI;
import edu.unc.mapseq.module.sequencing.samtools.SAMToolsIndexCLI;

public class WorkflowTest {

    @Test
    public void createDot() {

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(CondorJobEdge.class);

        int count = 0;

        // new job
        CondorJob writeVCFHeaderJob = new CondorJobBuilder().name(String.format("%s_%d", WriteVCFHeaderCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(writeVCFHeaderJob);

        // new job
        CondorJob fastQCR1Job = new CondorJobBuilder().name(String.format("%s_%d", FastQCCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(fastQCR1Job);

        // new job
        CondorJob bwaAlignR1Job = new CondorJobBuilder().name(String.format("%s_%d", BWAAlignCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(bwaAlignR1Job);
        graph.addEdge(fastQCR1Job, bwaAlignR1Job);

        // new job
        CondorJob fastQCR2Job = new CondorJobBuilder().name(String.format("%s_%d", FastQCCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(fastQCR2Job);

        // new job
        CondorJob bwaAlignR2Job = new CondorJobBuilder().name(String.format("%s_%d", BWAAlignCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(bwaAlignR2Job);
        graph.addEdge(fastQCR2Job, bwaAlignR2Job);

        // new job
        CondorJob bwaSAMPairedEndJob = new CondorJobBuilder().name(String.format("%s_%d", BWASAMPairedEndCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(bwaSAMPairedEndJob);
        graph.addEdge(bwaAlignR1Job, bwaSAMPairedEndJob);
        graph.addEdge(bwaAlignR2Job, bwaSAMPairedEndJob);

        // new job
        CondorJob picardAddOrReplaceReadGroupsJob = new CondorJobBuilder()
                .name(String.format("%s_%d", PicardAddOrReplaceReadGroupsCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(picardAddOrReplaceReadGroupsJob);
        graph.addEdge(bwaSAMPairedEndJob, picardAddOrReplaceReadGroupsJob);

        // new job
        CondorJob samtoolsIndexJob = new CondorJobBuilder().name(String.format("%s_%d", SAMToolsIndexCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(samtoolsIndexJob);
        graph.addEdge(picardAddOrReplaceReadGroupsJob, samtoolsIndexJob);

        // new job
        CondorJob picardMarkDuplicatesJob = new CondorJobBuilder()
                .name(String.format("%s_%d", PicardMarkDuplicatesCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(picardMarkDuplicatesJob);
        graph.addEdge(samtoolsIndexJob, picardMarkDuplicatesJob);

        // new job
        samtoolsIndexJob = new CondorJobBuilder().name(String.format("%s_%d", SAMToolsIndexCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(samtoolsIndexJob);
        graph.addEdge(picardMarkDuplicatesJob, samtoolsIndexJob);

        // new job
        CondorJob gatkRealignTargetCreatorJob = new CondorJobBuilder()
                .name(String.format("%s_%d", GATKRealignerTargetCreatorCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(gatkRealignTargetCreatorJob);
        graph.addEdge(samtoolsIndexJob, gatkRealignTargetCreatorJob);

        // new job
        CondorJob gatkIndelRealignerJob = new CondorJobBuilder().name(String.format("%s_%d", GATKIndelRealignerCLI.class.getSimpleName(), ++count))
                .build();
        graph.addVertex(gatkIndelRealignerJob);
        graph.addEdge(gatkRealignTargetCreatorJob, gatkIndelRealignerJob);

        // new job
        CondorJob picardFixMateJob = new CondorJobBuilder().name(String.format("%s_%d", PicardFixMateCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(picardFixMateJob);
        graph.addEdge(gatkIndelRealignerJob, picardFixMateJob);

        // new job
        samtoolsIndexJob = new CondorJobBuilder().name(String.format("%s_%d", SAMToolsIndexCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(samtoolsIndexJob);
        graph.addEdge(picardFixMateJob, samtoolsIndexJob);

        // new job
        CondorJob gatkCountCovariatesJob = new CondorJobBuilder().name(String.format("%s_%d", GATKCountCovariatesCLI.class.getSimpleName(), ++count))
                .build();
        graph.addVertex(gatkCountCovariatesJob);
        graph.addEdge(samtoolsIndexJob, gatkCountCovariatesJob);

        // new job
        CondorJob gatkTableRecalibrationJob = new CondorJobBuilder()
                .name(String.format("%s_%d", GATKTableRecalibrationCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(gatkTableRecalibrationJob);
        graph.addEdge(gatkCountCovariatesJob, gatkTableRecalibrationJob);

        // new job
        samtoolsIndexJob = new CondorJobBuilder().name(String.format("%s_%d", SAMToolsIndexCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(samtoolsIndexJob);
        graph.addEdge(gatkTableRecalibrationJob, samtoolsIndexJob);

        // new job
        CondorJob samtoolsFlagstatJob = new CondorJobBuilder().name(String.format("%s_%d", SAMToolsFlagstatCLI.class.getSimpleName(), ++count))
                .build();
        graph.addVertex(samtoolsFlagstatJob);
        graph.addEdge(samtoolsIndexJob, samtoolsFlagstatJob);

        // new job
        CondorJob gatkFlagstatJob = new CondorJobBuilder().name(String.format("%s_%d", GATKFlagStatCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(gatkFlagstatJob);
        graph.addEdge(samtoolsIndexJob, gatkFlagstatJob);

        // new job
        CondorJob gatkDepthOfCoverageJob = new CondorJobBuilder().name(String.format("%s_%d", GATKDepthOfCoverageCLI.class.getSimpleName(), ++count))
                .build();
        graph.addVertex(gatkDepthOfCoverageJob);
        graph.addEdge(samtoolsFlagstatJob, gatkDepthOfCoverageJob);
        graph.addEdge(gatkFlagstatJob, gatkDepthOfCoverageJob);

        // new job
        CondorJob gatkUnifiedGenotyperJob = new CondorJobBuilder()
                .name(String.format("%s_%d", GATKUnifiedGenotyperCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(gatkUnifiedGenotyperJob);
        graph.addEdge(gatkDepthOfCoverageJob, gatkUnifiedGenotyperJob);

        // new job
        CondorJob filterVariant1Job = new CondorJobBuilder().name(String.format("%s_%d", FilterVariantCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(filterVariant1Job);
        graph.addEdge(gatkUnifiedGenotyperJob, filterVariant1Job);

        // new job
        CondorJob gatkVariantRecalibratorJob = new CondorJobBuilder()
                .name(String.format("%s_%d", GATKVariantRecalibratorCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(gatkVariantRecalibratorJob);
        graph.addEdge(filterVariant1Job, gatkVariantRecalibratorJob);

        // new job
        CondorJob gatkApplyRecalibrationJob = new CondorJobBuilder()
                .name(String.format("%s_%d", GATKApplyRecalibrationCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(gatkApplyRecalibrationJob);
        graph.addEdge(gatkVariantRecalibratorJob, gatkApplyRecalibrationJob);

        CondorJobVertexNameProvider vnp = new CondorJobVertexNameProvider();
        CondorDOTExporter<CondorJob, CondorJobEdge> dotExporter = new CondorDOTExporter<CondorJob, CondorJobEdge>(vnp, vnp, null, null, null, null);
        File srcSiteResourcesImagesDir = new File("../src/site/resources/images");
        if (!srcSiteResourcesImagesDir.exists()) {
            srcSiteResourcesImagesDir.mkdirs();
        }
        File dotFile = new File(srcSiteResourcesImagesDir, "workflow.dag.dot");
        try {
            FileWriter fw = new FileWriter(dotFile);
            dotExporter.export(fw, graph);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
