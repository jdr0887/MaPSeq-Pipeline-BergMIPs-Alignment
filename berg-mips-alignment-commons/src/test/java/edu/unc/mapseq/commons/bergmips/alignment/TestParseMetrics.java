package edu.unc.mapseq.commons.bergmips.alignment;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.cxf.helpers.FileUtils;
import org.junit.Test;

public class TestParseMetrics {

    private static final List<String> keyList = Arrays.asList("BAIT_SET", "GENOME_SIZE", "BAIT_TERRITORY", "TARGET_TERRITORY",
            "BAIT_DESIGN_EFFICIENCY", "TOTAL_READS", "PF_READS", "PF_UNIQUE_READS", "PCT_PF_READS", "PCT_PF_UQ_READS PF", "PF_UQ_READS_ALIGNED",
            "PCT_PF_UQ_READS_ALIGNED", "PF_BASES_ALIGNED", "PF_UQ_BASES_ALIGNED", "ON_BAIT_BASES", "NEAR_BAIT_BASES", "OFF_BAIT_BASES",
            "ON_TARGET_BASES", "PCT_SELECTED_BASES", "PCT_OFF_BAIT", "ON_BAIT_VS_SELECTED", "MEAN_BAIT_COVERAGE", "MEAN_TARGET_COVERAGE",
            "MEDIAN_TARGET_COVERAGE", "PCT_USABLE_BASES_ON_BAIT", "PCT_USABLE_BASES_ON_TARGET", "FOLD_ENRICHMENT", "ZERO_CVG_TARGETS_PCT",
            "PCT_EXC_DUPE", "PCT_EXC_MAPQ", "PCT_EXC_BASEQ", "PCT_EXC_OVERLAP", "PCT_EXC_OFF_TARGET", "FOLD_80_BASE_PENALTY", "PCT_TARGET_BASES_1X",
            "PCT_TARGET_BASES_2X", "PCT_TARGET_BASES_10X", "PCT_TARGET_BASES_20X", "PCT_TARGET_BASES_30X", "PCT_TARGET_BASES_40X",
            "PCT_TARGET_BASES_50X", "PCT_TARGET_BASES_100X", "HS_LIBRARY_SIZE", "HS_PENALTY_10X", "HS_PENALTY_20X", "HS_PENALTY_30X",
            "HS_PENALTY_40X", "HS_PENALTY_50X", "HS_PENALTY_100X", "AT_DROPOUT", "GC_DROPOUT", "HET_SNP_SENSITIVITY", "HET_SNP_Q");

    @Test
    public void test() throws Exception {
        File f = new File("/tmp", "jdr-test.mem.rg.hs.metrics");
        List<String> lines = FileUtils.readLines(f);
        Iterator<String> lineIter = lines.iterator();

        String dataLine = null;
        while (lineIter.hasNext()) {
            String line = lineIter.next();
            if (line.startsWith("## METRICS CLASS")) {
                lineIter.next();
                dataLine = lineIter.next();
                break;
            }
        }
        String[] dataArray = dataLine.split("\t");

        for (int i = 0; i < keyList.size(); i++) {

            String key = keyList.get(i);
            String value = dataArray[i];
            System.out.printf("%s=%s%n", key, value);

        }

    }

}
