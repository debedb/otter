package com.enremmeta.otter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import com.enremmeta.otter.entity.Dataset;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class CdhConnection {
	private JSch jsch;

	private void log(String s) {
		Logger.log("CDHC> " + s);
	}

	private CdhConnection() {
		super();
	}

	private static CdhConnection cdhc = new CdhConnection();

	public static CdhConnection getInstance() {
		return cdhc;
	}
	
	private String getShellOutput() throws IOException {
		return getShellOutput(true);
	}

	private String getShellOutput(boolean waitForPrompt) throws IOException {
		String curPrompt = "OTTER" + this.userStack.size() + ">";
		String retval = "";
		boolean first = false;
		while (true) {
			int bytesToRead = this.is.available();
			if (bytesToRead == 0) {
				if (!first && !waitForPrompt) {
					break;
				}
				try {
					Thread.sleep(1000);
					first = false;
				} catch (InterruptedException ie) {
				}
				if (retval.endsWith(curPrompt)) {
					break;
				}
 				continue;
			}
			byte[] bytes = new byte[bytesToRead];
			int read = this.is.read(bytes);
			if (read < 0) {
				break;
			}
			String line = new String(bytes);
			retval += line;
			log("Received: " + line);
		}
		return retval;
	}

	private void shellCommand(String cmd, boolean waitForPrompt) throws IOException {
		getShellOutput(false);
		log("Sending: " + cmd);
		cmd += "\n";
		this.os.write(cmd.getBytes());
		this.os.flush();
		if (waitForPrompt) {
			log("Waiting for prompt " + getPrompt());
		}
		getShellOutput(waitForPrompt);
	}

	private void execCommandAs(String cmd) throws IOException, JSchException {
		execCommandAs(cmd, Constants.HDFS_USER);
	}

	private void execCommandAs(String cmd, String user) throws IOException,
			JSchException {
		cmd = "sudo su -l " + user + " -c '" + cmd + "'";
		execCommand(cmd);
	}

	private void execCommand(String cmd) throws IOException, JSchException {
		getShellOutput();
		log("Sending: " + cmd);

		// cmd += "\n";
		// this.os.write(cmd.getBytes());
		// this.os.flush();
		// exec 'scp -t rfile' remotely

		Channel channel = sess.openChannel("exec");
		((ChannelExec) channel).setCommand(cmd);

		OutputStream curOut = channel.getOutputStream();
		InputStream curIn = channel.getInputStream();
		channel.connect();
		byte[] tmp = new byte[1024];

		while (true) {
			while (curIn.available() > 0) {
				int i = curIn.read(tmp, 0, 1024);
				if (i < 0)
					break;
				System.out.print(new String(tmp, 0, i));
			}
			if (channel.isClosed()) {
				if (curIn.available() > 0) {
					continue;
				}
				log("exit-status: " + channel.getExitStatus());
				break;
			}
			try {
				Thread.sleep(1000);
			} catch (Exception ee) {
			}
		}
		channel.disconnect();

		getShellOutput();
	}

	private List<String> userStack = new ArrayList<String>();

	private List<String> promptStack = new ArrayList<String>();

	private void exitShell() throws IOException {
		// log("Users on stack before exit: " + userStack.toString());
		shellCommand("exit", false);
		shellCommand("whoami", false);
		userStack.remove(userStack.size() - 1);
		setPrompt();
		// log("Users on stack after exit: " + userStack.toString());
	}

	private void pushUser(String user) {
		userStack.add(user);
		// log("Users on stack: " + userStack.toString());
	}

	private void sudoCdhUser() throws Exception {
		shellCommand("sudo su -", false);
		shellCommand("whoami", false);
		pushUser("root");
		setPrompt();
		shellCommand("su - " + cdhUser, false);
		shellCommand("whoami", false);
		pushUser(cdhUser);
		setPrompt();
	}

	private void popSudos() throws Exception {
		exitShell();
		exitShell();
	}

	// java.lang.IllegalArgumentException: AWS Access Key ID and Secret Access
	// Key must be specified as the username or password (respectively) of a s3n
	// URL, or by setting the fs.s3n.awsAccessKeyId or fs.s3n.awsSecretAccessKey
	// properties (respectively).

	// sudo su -l hdfs -c 'pwd'
	public void loadDataFromS3(String bucket, String path, String accessKey,
			String secretKey, String tableName) throws OtterException {
		// hadoop distcp
		try {
			log("loadDataFromS3(" + bucket + ", " + path +", " + accessKey + ", SECRET_KEY, " + tableName + ")");
			sudoCdhUser();
			String myFname = new File(path).getName() + "_"
					+ System.currentTimeMillis();
			String cmd = "hadoop distcp " + " -Dfs.s3n.awsAccessKeyId="
					+ accessKey + " -Dfs.s3n.awsSecretAccessKey=" + secretKey
					+ " s3n://" + bucket + path + " hdfs:"
					+ Constants.OTTER_HDFS_PREFIX + tableName + "/" + myFname;
			shellCommand(cmd, true);
			popSudos();
		} catch (Exception e) {
			throw new OtterException(e);
		}
	}

	/**
	 * Creates a new dataset.
	 */
	public void addDataset(Dataset ds) throws Exception {
		sudoCdhUser();
		shellCommand("hadoop fs -mkdir hdfs:" + Constants.OTTER_HDFS_PREFIX
				+ ds.getName(), true);
		
		popSudos();
	}

	// hadoop fs -mkdir hdfs:/user/oy/txns

	public void loadData(Dataset ds, String fname) throws Exception {
		sudoCdhUser();
		String uploadPath = Config.getInstance().getProperty(
				Config.PROP_CDH_UPLOAD_PATH);
		shellCommand("hadoop fs -copyFromLocal " + uploadPath + fname
				+ " hdfs:" + Constants.OTTER_HDFS_PREFIX + ds.getName(), true);
	}

	public void testCleanup() throws OtterException {
		try {
			sudoCdhUser();
			shellCommand("hadoop fs -rm -r -f hdfs:"
					+ Constants.OTTER_HDFS_PREFIX + "test1", true);
			popSudos();
		} catch (Exception e) {
			throw new OtterException(e);
		}
	}

	private Session sess;
	private String cdhUser;

	public void connect() throws IOException, JSchException {
		Config config = Config.getInstance();
		String cdhHost = config.getProperty(Config.PROP_CDH_HOST);
		String keyFilePath = config.getProperty(Config.PROP_CDH_SSH_KEY_PATH);
		String user = config.getProperty(Config.PROP_CDH_SSH_USER);

		Logger.log("Connecting to " + cdhHost + " as " + user + "...");
		cdhUser = config.getProperty(Config.PROP_CDH_UNIX_USER);
		if (jsch == null) {
			jsch = new JSch();
			// String privKey = new String(Files.readAllBytes(Paths
			// .get(keyFilePath)), StandardCharsets.UTF_8);
			jsch.addIdentity(keyFilePath);
		}
		if (sess == null || !sess.isConnected()) {
			sess = jsch.getSession(user, cdhHost);

			// TODO
			java.util.Properties props = new java.util.Properties();
			props.put("StrictHostKeyChecking", "no");
			sess.setConfig(props);
			sess.connect();
		}
		if (shellChannel == null) {
			shellChannel = sess.openChannel("shell");
			shellChannel.connect();
			is = shellChannel.getInputStream();
			os = shellChannel.getOutputStream();
			Logger.log("Opened shell.");
		}

		Logger.log("Connected to " + cdhHost + " as " + user + ".");
		setPrompt();
	}
	
	private String getPrompt() {
		String prompt = "OTTER" + userStack.size() + ">";
		return prompt;
	}
	
	private void setPrompt() throws IOException {
		String prompt = getPrompt();
		log("Setting prompt to " + prompt);
		String cmd ="export PS1='" + prompt + "'\r\n";
		getShellOutput(false);
		this.os.write(cmd.getBytes());
		this.os.write("echo\n".getBytes());
		this.os.flush();
		getShellOutput(false);
	}


	private static String TEST_COMMAND = "echo TEST_TEST_TEST\r\n";
	private static String TEST_STRING = "TEST_TEST_TEST";
	private String prompt;

	private int checkAck(InputStream in) throws IOException {
		int b = in.read();
		// b may be 0 for success,
		// 1 for error,
		// 2 for fatal error,
		// -1
		if (b == 0)
			return b;
		if (b == -1)
			return b;

		if (b == 1 || b == 2) {
			StringBuffer sb = new StringBuffer();
			int c;
			do {
				c = in.read();
				sb.append((char) c);
			} while (c != '\n');
			if (b == 1) { // error
				System.out.print(sb.toString());
			}
			if (b == 2) { // fatal error
				System.out.print(sb.toString());
			}
		}
		return b;
	}

	private Channel shellChannel;

	private InputStream is;

	private OutputStream os;

	private BufferedWriter bw;

	private BufferedReader br;

	public void upload(String lfile) throws IOException, JSchException {
		String uploadDir = Config.getInstance().getProperty(
				Config.PROP_CDH_UPLOAD_PATH);
		String fileName = new File(lfile).getName();
		String rfile = new File(uploadDir, fileName).getAbsolutePath();

		boolean ptimestamp = true;

		// exec 'scp -t rfile' remotely
		String command = "scp " + (ptimestamp ? "-p" : "") + " -t " + rfile;
		Channel channel = sess.openChannel("exec");
		((ChannelExec) channel).setCommand(command);

		// get I/O streams for remote scp
		OutputStream out = channel.getOutputStream();
		InputStream in = channel.getInputStream();
		channel.connect();

		int chk = checkAck(in);
		if (chk != 0) {
			throw new IOException("checkAck() returned " + chk);
		}

		File _lfile = new File(lfile);

		if (ptimestamp) {
			command = "T " + (_lfile.lastModified() / 1000) + " 0";
			// The access time should be sent here,
			// but it is not accessible with JavaAPI ;-<
			command += (" " + (_lfile.lastModified() / 1000) + " 0\n");
			out.write(command.getBytes());
			out.flush();

			chk = checkAck(in);
			if (chk != 0) {
				throw new IOException("checkAck() returned " + chk);
			}

		}

		// send "C0644 filesize filename", where filename should not include '/'
		long filesize = _lfile.length();
		command = "C0644 " + filesize + " ";
		if (lfile.lastIndexOf('/') > 0) {
			command += lfile.substring(lfile.lastIndexOf('/') + 1);
		} else {
			command += lfile;
		}
		command += "\n";
		out.write(command.getBytes());
		out.flush();

		chk = checkAck(in);
		if (chk != 0) {
			throw new IOException("checkAck() returned " + chk);
		}

		FileInputStream fis = null;
		// send a content of lfile
		fis = new FileInputStream(lfile);
		byte[] buf = new byte[1024];
		while (true) {
			int len = fis.read(buf, 0, buf.length);
			if (len <= 0)
				break;
			out.write(buf, 0, len); // out.flush();
		}
		fis.close();
		fis = null;
		// send '\0'
		buf[0] = 0;
		out.write(buf, 0, 1);
		out.flush();

		chk = checkAck(in);
		if (chk != 0) {
			throw new IOException("checkAck() returned " + chk);
		}

		out.close();

		channel.disconnect();

	}
}
