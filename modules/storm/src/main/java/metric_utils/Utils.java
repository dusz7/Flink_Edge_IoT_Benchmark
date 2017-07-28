package metric_utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Utils {
	public static double getInputRate(String inputFile) {
		return getRate(inputFile, 3, 5);
	}

	public static double getThroughput(String outFile) {
		return getRate(outFile, 3, 4);
	}

	public static double getRate(String file, int tsIndex, int msgIdIndex) {
		double rate = 0;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String[] first = reader.readLine().split(",");
			String last = null, line = null;
			while ((line = reader.readLine()) != null) {
				last = line;
			}
			String[] lastSplit = last.split(",");
			long initTs = Long.parseLong(first[tsIndex]);
			long initMsg = Long.parseLong(first[msgIdIndex]);
			long finalTs = Long.parseLong(lastSplit[tsIndex]);
			long finalMsg = Long.parseLong(lastSplit[msgIdIndex]);
			rate = (double) (finalMsg - initMsg) / ((finalTs - initTs) / 1000);
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return rate;
	}

	public static double getLatency(String inFile, String oFile, int inFileTsIndex, int inFileMsgIndex,
			int outFileTsIndex, int outFileMsgIndexs) {
		
		return 0;
	}

}
