package com.assignment;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/*
 * Steps to run
 * hadoop jar UAER_DATA.jar /mnt/home/edureka_1142691/input/user.csv /user/edureka_1142691/assignment/user.csv
 */
public class UserDataApp {

	public static void main(String[] args) throws IOException, ParseException {
		String hdfsPath = "hdfs://nameservice1", source = "", dest = "";
		Configuration conf;
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.println("Enter local source and HDFS destination paths...");
		source = br.readLine();
		dest = br.readLine();
		conf = new Configuration();
		conf.set("fs.default.name", hdfsPath);
		FileSystem fileSystem = FileSystem.get(conf);
		Path destPath = new Path(dest);
		if (fileSystem.exists(destPath)) {
			fileSystem.delete(destPath, true);
		}
		addFile(source, destPath, fileSystem);
		processUserData(destPath, fileSystem);
		fileSystem.close();
	}

	public static void addFile(String source, Path destPath, FileSystem fileSystem) throws IOException, ParseException {
		FSDataOutputStream out = fileSystem.create(destPath);
		InputStream in = new BufferedInputStream(new FileInputStream(new File(source)));
		byte[] b = new byte[1024];
		int numBytes = 0;
		while ((numBytes = in.read(b)) > 0) {
			out.write(b, 0, numBytes);
		}
		in.close();
		out.close();
	}

	private static void processUserData(Path sourcePath, FileSystem fileSystem) throws IOException, ParseException {
		if (fileSystem.exists(sourcePath)) {
			List<User> users = getUserRecordsFromHDFS(sourcePath, fileSystem);
			Double aggSal = aggregateSalary(users);
			Map<String, Integer> userNameFreq = getUserNameWithFrequency(users);
			String userNameMode = calculateModeForUserName(userNameFreq);
			System.out.println("Aggregated salary :: " + aggSal);
			System.out.println("userNameMode :: " + userNameMode);

		} else {
			System.out.println("source path doesnt exists" + sourcePath);
		}
	}

	private static List<User> getUserRecordsFromHDFS(Path sourcePath, FileSystem fileSystem)
			throws IOException, ParseException {
		List<User> users = new ArrayList<User>();
		FSDataInputStream in = fileSystem.open(sourcePath);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String header = br.readLine(), line;
		while ((line = br.readLine()) != null) {
			String[] record = line.split(",");
			User userRecord = new User(record[0], Integer.parseInt(record[1]), record[2],
					Double.parseDouble(record[1]));
			users.add(userRecord);
		}
		in.close();
		return users;
	}

	private static Double aggregateSalary(List<User> users) {
		Double aggSal = 0.0;
		for (User userRecord : users) {
			aggSal = aggSal + userRecord.getSalary();
		}
		return aggSal;
	}

	private static Map<String, Integer> getUserNameWithFrequency(List<User> users) {
		Map<String, Integer> nameCountMap = new HashMap<>();
		for (User userRecord : users) {
			Integer count = 1;
			if (nameCountMap.containsKey(userRecord.getUserName())) {
				count = nameCountMap.get(userRecord.getUserName()) + 1;
			}
			nameCountMap.put(userRecord.getUserName().toLowerCase(), count);
		}
		return nameCountMap;
	}

	private static String calculateModeForUserName(Map<String, Integer> userNameFreq) {
		int maxMode = 0;
		String userNameMode = "";
		for (Map.Entry<String, Integer> entry : userNameFreq.entrySet()) {
			if (maxMode < entry.getValue()) {
				maxMode = entry.getValue();
				userNameMode = entry.getKey();
			}
		}
		return userNameMode;
	}

}
