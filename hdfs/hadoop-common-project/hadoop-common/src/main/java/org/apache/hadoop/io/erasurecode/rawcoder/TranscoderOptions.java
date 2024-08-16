package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.io.erasurecode.ECSchema;

public final class TranscoderOptions {
    private final int newDataNum;
    private final int newParityNum;
    private final ECSchema oldSchema;
    private final int oldDataNum;
    private final int mergeFactor;


    public TranscoderOptions(int newDataNum, int newParityNum, ECSchema oldSchema){
        this.newDataNum = newDataNum;
        this.newParityNum = newParityNum;
        this.oldSchema = oldSchema;
        this.oldDataNum = oldSchema.getNumDataUnits();
        this.mergeFactor = newDataNum/oldDataNum;

    }

    public int getNewDataNum() {
        return newDataNum;
    }

    public int getOldDataNum() {
        return oldDataNum;
    }

    public int getNewParityNum() {
        return newParityNum;
    }

    public int getMergeFactor() {
        return mergeFactor;
    }

    public int getAllNewUnits(){
        return newDataNum + newParityNum;
    }
}
