package org.apache.uima.ducc.database;

import java.util.ArrayList;
import java.util.Map;

import org.apache.uima.ducc.common.utils.id.DuccId;


/**
 * This is a helper to serialize the ProcessToJob map in the OR checkpoint.  We need this because
 * the key for that map is complex (a DuccId) and it can't be propertly serialized into a JSON
 * dictionary key.
 *
 */
class ProcessToJobList
{
    ArrayList<PjPair> l = new ArrayList<PjPair>();

    ProcessToJobList() {};
    ProcessToJobList(Map<DuccId, DuccId> m)
    {
        for ( DuccId k : m.keySet() ) {
            l.add(new PjPair(k, m.get(k)));
        }
    }

    void fill(Map<DuccId, DuccId> ptj)
    {
        for ( PjPair p : l ) ptj.put(p.k, p.v);
    }

    static private class PjPair
    {
        DuccId k;
        DuccId v;
        
        PjPair(DuccId k, DuccId v) { this.k = k; this.v = v; }
        
    }

}
