package org.apache.hadoop.hdfs.server.ectransitioner;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ECSchema;

import static org.apache.hadoop.io.erasurecode.ErasureCodeConstants.RS_CODEC_NAME;
import static org.apache.hadoop.io.erasurecode.ErasureCodeConstants.XOR_CODEC_NAME;

@InterfaceAudience.Private
final public class ECTransitionerParameters {

    private final String fileName;
    private final ECSchema targetSchema;

    String getFileName() {
        return fileName;
    }

    public ECSchema getTargetSchema() {
        return targetSchema;
    }

    private ECTransitionerParameters() {
        this(new Builder());
    }

    private ECTransitionerParameters(Builder builder) {
        this.fileName = builder.fileName;
        this.targetSchema = builder.targetSchema;
    }

    static class Builder {
        // Defaults
        private String fileName;
        private ECSchema targetSchema = new ECSchema(RS_CODEC_NAME, 3, 2);

        Builder() {
        }

        void setFileName(String fn) {
            this.fileName = fn;
        }

        void setTargetSchema(String codec, int numDataUnits, int numParityUnits) {
            this.targetSchema = new ECSchema(codec, numDataUnits, numParityUnits);
        }

        ECTransitionerParameters build() {
            return new ECTransitionerParameters(this);
        }
    }
}
