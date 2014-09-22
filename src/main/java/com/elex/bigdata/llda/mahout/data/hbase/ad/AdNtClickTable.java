package com.elex.bigdata.llda.mahout.data.hbase.ad;

import com.elex.bigdata.llda.mahout.data.hbase.RecordUnit;
import com.elex.bigdata.llda.mahout.data.hbase.ResultParser;
import com.elex.bigdata.util.MetricMapping;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

public class AdNtClickTable extends AdTable {
    private byte[] CATEGORY = Bytes.toBytes("c"); //广告
    private byte[] typeCol = Bytes.toBytes("t");
    private byte[] scoreCf = Bytes.toBytes("h");
    //pId_1+nt_2+time_8+uid_
    private int UID_INDEX = 11, NT_INDEX_START = 1, NT_INDEX_END = 3;
    private static Map<Byte,String> projects;
    static{
        projects = new HashMap<Byte, String>();
        Map<String,Byte> projectsName = MetricMapping.getInstance().getAllProjectShortNameMapping();
        for(Map.Entry<String,Byte> pn : projectsName.entrySet()){
            projects.put(pn.getValue(),pn.getKey());
        }
    }


    @Override
    public Scan getScan(long startTime, long endTime) {
        Map<String, List<String>> familyColumns = new HashMap<String, List<String>>();

        List<String> columns = new ArrayList<String>();
        columns.add(Bytes.toString(CATEGORY));
        familyColumns.put(Bytes.toString(family), columns);

        columns = new ArrayList<String>();
        columns.add(Bytes.toString(typeCol));
        familyColumns.put(Bytes.toString(scoreCf), columns);

        return getScan(familyColumns, startTime, endTime);
    }

    @Override
    public ResultParser getResultParser() {
        return new AdNtResultParser();
    }

    private class AdNtResultParser implements ResultParser {

        @Override
        public List<RecordUnit> parse(Result result) {
            byte[] rk = result.getRow();
            String uid = Bytes.toString(Arrays.copyOfRange(rk, UID_INDEX, rk.length));
            String nt = Bytes.toString(Arrays.copyOfRange(rk, NT_INDEX_START, NT_INDEX_END));
            String cat =Bytes.toString(result.getValue(family, CATEGORY));
            String type = "\\N";
            byte[] typeByte = result.getValue(scoreCf,typeCol);
            if(typeByte != null){
                type = Bytes.toString(typeByte);
            }

            String p = projects.get(rk[0]);
            List<RecordUnit> recordUnits = new ArrayList<RecordUnit>();
            //uid \t nt \t cat \t type \t p
            String field = p + "\t" + nt + "\t" + cat + "\t" + type;
            recordUnits.add(new RecordUnit(uid, field));
            return recordUnits;
        }
    }
}
