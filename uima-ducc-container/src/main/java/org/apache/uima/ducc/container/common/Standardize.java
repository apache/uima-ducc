package org.apache.uima.ducc.container.common;

public class Standardize {

	public enum Label {
		state,
		event,
		curr,
		prev,
		hash,
		loaded,
		seqNo,
		transNo,
		remote,
		action,
		type,
		AckMsecs,
		EndMsecs,
		;
		
		Label() {
		}
		
		public String get() {
			return this+"=";
		}
	}
}
