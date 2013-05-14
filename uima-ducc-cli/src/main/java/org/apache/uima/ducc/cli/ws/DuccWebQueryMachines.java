package org.apache.uima.ducc.cli.ws;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Iterator;
import java.util.List;

import org.apache.uima.ducc.cli.ws.json.MachineFacts;
import org.apache.uima.ducc.cli.ws.json.MachineFactsList;

import com.google.gson.Gson;

public class DuccWebQueryMachines extends DuccWebQuery {

	private DuccWebQueryMachines() {
		super("/ducc-servlet/json-format-machines");
	}

	private MachineFactsList get() throws Exception {
		URL url = new URL(getUrlString());
		URLConnection ucon = url.openConnection();
		BufferedReader br = new BufferedReader(new InputStreamReader(ucon.getInputStream()));
		String jSon = br.readLine();
		Gson gson = new Gson();
		MachineFactsList fl = gson.fromJson(jSon, MachineFactsList.class);
        br.close();
        return fl;
	}
	
	private String stringify(List<String> list) {
		StringBuffer sb = new StringBuffer();
		for(String item : list) {
			if(sb.length() > 0) {
				sb.append(",");
			}
			sb.append(item);
		}
		return "["+sb.toString()+"]";
	}
	
	private void display(MachineFactsList fl) {
		if(fl != null) {
			Iterator<MachineFacts> fIterator = fl.iterator();
			while(fIterator.hasNext()) {
				MachineFacts f = fIterator.next();
				System.out.println(f.name);
				System.out.println("  "+"status: "+f.status);
				System.out.println("  "+"aliens: "+ stringify(f.aliens));
				System.out.println("  "+"swap: "+ f.swap);
				System.out.println("  "+"reserve: "+ f.reserve);
				System.out.println("  "+"memory: "+ f.memory);
				System.out.println("  "+"sharesTotal: "+ f.sharesTotal);
				System.out.println("  "+"sharesInuse: "+ f.sharesInuse);
				System.out.println("  "+"ip: "+ f.ip);
				System.out.println("  "+"memory: "+ f.memory);
				System.out.println("  "+"heartbeat: "+ f.heartbeat);
			}
		}
		else {
			System.out.println("?");
		}
	}
	
	private void main_instance(String[] args) throws Exception {
		MachineFactsList fl = get();
		display(fl);
	}
	
	public static void main(String[] args) {
		try {
			DuccWebQueryMachines dwq = new DuccWebQueryMachines();
			dwq.main_instance(args);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}