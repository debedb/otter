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
		String retval = "";
		while (true) {
			int bytesToRead = this.is.available();
			if (bytesToRead == 0) {
				break;
			}
			byte[] bytes = new byte[bytesToRead];
			this.is.read(bytes);
			String line = new String(bytes);
			retval += line;
			log("Received: " + line);
		}
		return retval;
	}

	private void sendCommand(String cmd) throws IOException {
		getShellOutput();
		log("Sending: " + cmd);
		cmd += "\n";
		this.os.write(cmd.getBytes());
		this.os.flush();
		getShellOutput();
	}

	private List<String> userStack = new ArrayList<String>();

	private void exitShell() throws IOException {
		log("Users on stack before exit: " + userStack.toString());
		sendCommand("exit");
		userStack.remove(userStack.size() - 1);
		log("Users on stack after exit: " + userStack.toString());
	}

	private void pushUser(String user) {
		userStack.add(user);
		log("Users on stack: " + userStack.toString());
	}

	private void sudoCdhUser() throws Exception {
		sendCommand("sudo su -");
		pushUser("root");
		sendCommand("su - " + cdhUser);
		pushUser(cdhUser);
	}

	private void popSudos() throws Exception {
		exitShell();
		exitShell();
	}

	// java.lang.IllegalArgumentException: AWS Access Key ID and Secret Access
	// Key must be specified as the username or password (respectively) of a s3n
	// URL, or by setting the fs.s3n.awsAccessKeyId or fs.s3n.awsSecretAccessKey
	// properties (respectively).

	public void loadDataFromS3(String bucket, String path, String accessKey,
			String secretKey, String tableName) throws OtterException {
		// hadoop distcp
		try {
			sudoCdhUser();
			String cmd = "hadoop distcp " + " -Dfs.s3n.awsAccessKeyId="
					+ accessKey + " -Dfs.s3n.awsSecretAccessKey=" + secretKey
					+ " s3n://" + bucket + path + "hdfs:"
					+ Constants.OTTER_HDFS_PREFIX + tableName;
			sendCommand(cmd);
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
		sendCommand("hadoop fs -mkdir hdfs:" + Constants.OTTER_HDFS_PREFIX
				+ ds.getName());
		popSudos();
	}

	// hadoop fs -mkdir hdfs:/user/oy/txns

	public void loadData(Dataset ds, String fname) throws Exception {
		sudoCdhUser();
		String uploadPath = Config.getInstance().getProperty(
				Config.PROP_CDH_UPLOAD_PATH);
		sendCommand("hadoop fs -copyFromLocal " + uploadPath + fname + " hdfs:"
				+ Constants.OTTER_HDFS_PREFIX + ds.getName());
	}

	public void testCleanup() throws OtterException {
		try {
			sudoCdhUser();
			sendCommand("hadoop fs -rmdir hdfs:" + Constants.OTTER_HDFS_PREFIX
					+ "test1");
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
	}

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
