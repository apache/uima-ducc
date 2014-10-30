package org.apache.uima.ducc.container.jd.test.helper;

import java.util.ArrayList;
import java.util.Random;

public class ThreadInfoFactory {

	private ArrayList<ThreadInfo> list = new ArrayList<ThreadInfo>();
	
	public ThreadInfoFactory(int nodes, int pids, int tids) {
		for(int i=0; i<nodes; i++) {
			for(int j=0; j<pids; j++) {
				for(int k=0; k<tids; k++) {
					int node = (i+1);
					int pid = (node*100)+(j+1);
					int tid = (k+1);
					ThreadInfo ti = new ThreadInfo(node, pid, tid);
					list.add(ti);
				}
			}
		}
	}
	
	public ThreadInfo getRandom() {
		ThreadInfo ti = null;
		if(list.size() > 0) {
			Random random = new Random();
			int n = list.size();
			int index = random.nextInt(n);
			ti = list.get(index);
		}
		return ti;
	}
}
