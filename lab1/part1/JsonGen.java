
public class JsonGen {

	private int msgId;
	private String user;
	private String msgStatus;
	private String recip;
	private String msgText;
	private int origMsg;
	private int numObjs;
	private String outFile;


	public JsonGen (int numObjs, String fileName) {
		this.numObjs = numObjs;
		this.outFile = fileName;
	}

	public String JSON_Object() {
		String obj = "{}";
		return obj;
	}
	

}