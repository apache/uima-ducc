package org.apache.uima.ducc.ps.service.processor.uima.utils;

public class PerformanceMetrics {

	private String name;
	private String uniqueName;
	private long analysisTime;

	/**
	 * Creates a performance metrics instance
	 * 
	 */
	public PerformanceMetrics(String name, String uimaContextPath, long analysisTime) {
		this.name = name;
		this.uniqueName = uimaContextPath;
		this.analysisTime = analysisTime;
	}

	/**
	 * Gets the local name of the component as specified in the aggregate
	 * 
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Gets the unique name of the component reflecting its location in the
	 * aggregate hierarchy
	 * 
	 * @return the unique name
	 */
	public String getUniqueName() {
		if (uniqueName != null && uniqueName.trim().length() > 0 && !uniqueName.trim().equals("Components")) {
			// if ( !uimaContextPath.endsWith(getName())) {
			// return uimaContextPath+"/"+getName();
			// }
			return uniqueName;
		} else {
			return getName();
		}
	}

	/**
	 * Gets the elapsed time the CAS spent analyzing this component
	 * 
	 * @return time in milliseconds
	 */
	public long getAnalysisTime() {
		return analysisTime;
	}

}
