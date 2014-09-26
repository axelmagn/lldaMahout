package com.elex.bigdata.llda.mahout.data.hbase.nav;

import com.elex.bigdata.llda.mahout.data.hbase.RecordUnit;
import com.elex.bigdata.llda.mahout.data.hbase.ResultParser;
import com.elex.bigdata.util.MetricMapping;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 9/2/14
 * Time: 3:49 PM
 * To change this template use File | Settings | File Templates.
 */
public class NavProjectNtTable extends NavTable {
    private byte[] URL = Bytes.toBytes("url");
    //pId_1+nt_2+time_14(yyyyMMddHHmmss)+uid_
    private int UID_INDEX = 17, NT_INDEX_START = 1, NT_INDEX_END = 2;
    private static Map<Byte, String> projects;

    static {
        projects = new HashMap<Byte, String>();
        Map<String, Byte> projectsName = MetricMapping.getInstance().getAllProjectShortNameMapping();
        for (Map.Entry<String, Byte> pn : projectsName.entrySet()) {
            projects.put(pn.getValue(), pn.getKey());
        }
    }

    @Override
    public Scan getScan(long startTime, long endTime) {
        List<String> columns = new ArrayList<String>();
        columns.add(Bytes.toString(URL));
        return getScan(startTime, endTime, columns);
    }

    @Override
    public ResultParser getResultParser() {
        return new NavNtResultParser();
    }

    private class NavNtResultParser implements ResultParser {

        @Override
        public List<RecordUnit> parse(Result result) {
            List<RecordUnit> recordUnits = new ArrayList<RecordUnit>();
            byte[] rk = result.getRow();
            String p = projects.get(rk[0]);
            recordUnits.add(new RecordUnit(Bytes.toString(Arrays.copyOfRange(rk, UID_INDEX, rk.length)), p + "\t" + Bytes.toString(rk, NT_INDEX_START, NT_INDEX_END)));
            return recordUnits;
        }
    }
}
