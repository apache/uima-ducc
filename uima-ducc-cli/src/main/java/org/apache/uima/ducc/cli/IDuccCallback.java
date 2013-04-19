package org.apache.uima.ducc.cli;

public interface IDuccCallback {

  /**
   * This method is called by relevant parts of the API with messages redirected from the remote
   * process.
   * 
   * @param pnum
   *          This is the callback number for the remote process e.g. 1 is assigned to the first
   *          process to call back
   * @param msg
   *          This is the logged message.
   */
  public void console(int pnum, String msg);

  /**
   * This method is called by relevant parts of the API with messages related to the status of the
   * submitted request.
   * 
   * @param msg
   *          This is the logged message.
   */
  public void status(String msg);

}
