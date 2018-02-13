import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

class InputData {
	public String CMTE_ID;
	public String name;
	public String zipCode;
	public String keyId;
	public Date transDate;
	public int transAmt;
	public String otherID;
	
	public InputData(String CMTE_ID, String name, String zipCode, Date transDate, int transAmt, String otherID) {
		this.CMTE_ID = CMTE_ID;
		this.name = name;
		this.zipCode = zipCode;
		this.keyId = name+"$"+zipCode;
		this.transDate = transDate;
		this.transAmt = transAmt;
		this.otherID = otherID;
	}

	@Override
	public String toString() {
		return this.CMTE_ID + "," + this.name + "," +  this.zipCode + 
				 "," + this.keyId + "," +  this.transDate + "," +  this.transAmt +
				  "," + this.otherID;
	}
	
	public static boolean checkFormat(InputData data) {
		//check otherid
		if(!data.otherID.equals("") || data.CMTE_ID.equals("") || data.transDate == null) {
			return false;
		}
		//check date
		if(data.transDate == null)
			return false;
		//check name
		if(data.name.length() > 200)
			return false;
		//check zipCode
		if(data.zipCode.length() < 5)
			return false;
		data.zipCode = data.zipCode.substring(0, 5);
		
		return true;
	}

}

class OutputData {
	//每一个独立的zipCode对应一个OutputData
	public String CMTE_ID;
	public String zipCode;
	public Date date;
	public int totalAmt;
	public int repeatNum; 
	public int percentile;
	public int percent_res;
	public List<Integer> list;
	
	
	public OutputData(String CMTE_ID, String zipCode, Date date, int percentile) {
		this.CMTE_ID = CMTE_ID;
		this.zipCode = zipCode;
		this.date = date;
		this.totalAmt = 0;
		this.repeatNum = 1;
		this.percentile = percentile;
		this.percent_res = 0;
		list = new LinkedList<>();
	}
	
	public Date getDate() {
		return date;
	}

	@Override
	public String toString() {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		
		StringBuilder sb = new StringBuilder();
		sb.append(CMTE_ID);
		sb.append("|");
		sb.append(zipCode);
		sb.append("|");
		sb.append(cal.get(Calendar.YEAR));
		sb.append("|");
		sb.append(percent_res);
		sb.append("|");
		sb.append(totalAmt);
		sb.append("|");
		sb.append(repeatNum);
		return sb.toString();
	}
	
	public void addNum(int num) {
		list.add(num);
	}

    // Returns the median of current data stream
    public int findPercentile() {
    		Collections.sort(list);
    		int a = (int)(list.size()/100*percentile);
    		int index = (int)Math.ceil((list.size()/100)*percentile);
        return list.get(index);
    }
	
}

public class DataUtil {
	public Map<String, InputData> record; // unique id - OutputData
	public Map<String, OutputData> map; // zipCode - OutputData
	public int percentile;
	public BufferedReader percent;
	public BufferedReader in;
	public PrintWriter out;
	
	public DataUtil() {
		map = new HashMap<>();
		record = new HashMap<>();
	}
	
	public void readIn(String inputFile, String percentFile, String outputFile) {
		try {
			percent = new BufferedReader(new FileReader(percentFile));
			percentile = Integer.parseInt(percent.readLine());
			in = new BufferedReader(new FileReader(inputFile));
			String line = null;
			out = new PrintWriter(outputFile, "UTF-8");
			
			while((line = in.readLine()) != null) {
				//read in line by line
				String[] arr = line.split("\\|");
				String CMTE_ID = arr[0];
				String name = arr[7];
				String zipCode = arr[10];
				DateFormat format = new SimpleDateFormat("MMddyyyy");
				Date transDate = (Date)format.parse(arr[13]);
				int transAmt = Integer.parseInt(arr[14]);
				String otherId = arr[15];
				InputData data = new InputData(CMTE_ID, name, zipCode, transDate, transAmt, otherId);
				if(InputData.checkFormat(data)) {
					//format correct
					dataProcess(data, out);
				}
			}
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				in.close();
				percent.close();
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}	
		}
	}
	
	public void dataProcess(InputData data, PrintWriter out) {
		if(!record.containsKey(data.keyId)) {
			//first record
			record.put(data.keyId, data);
		} else {
			//second or later record
			InputData tmpData = record.get(data.keyId);
			OutputData futureData = new OutputData("", "", null, percentile);
			if(tmpData.transDate.before(data.transDate)) {
				//new record is later
				futureData.CMTE_ID = data.CMTE_ID;
				futureData.zipCode = data.zipCode;
				futureData.date = data.transDate;
				if(!map.containsKey(data.zipCode)) {
					//first repeat
					futureData.totalAmt += data.transAmt;
					futureData.repeatNum = 1;
				} else {
					//second or later repeat
					futureData.totalAmt = map.get(data.zipCode).totalAmt + data.transAmt;
					futureData.repeatNum = map.get(data.zipCode).repeatNum + 1;
					futureData.list = map.get(tmpData.zipCode).list;
				}
				futureData.addNum(data.transAmt);
				futureData.percent_res = futureData.findPercentile();
				map.put(data.zipCode, futureData);
				out.println(futureData.toString());
			} else {
				//old record is later, update the record
				futureData.CMTE_ID = tmpData.CMTE_ID;
				futureData.zipCode = tmpData.zipCode;
				futureData.date = tmpData.transDate;
				if(!map.containsKey(tmpData.zipCode)) {
					//first repeat
					futureData.totalAmt += tmpData.transAmt;
					futureData.repeatNum = 1;
				} else {
					//second repeat or later
					futureData.totalAmt = map.get(tmpData.zipCode).totalAmt + tmpData.transAmt;
					futureData.repeatNum = map.get(tmpData.zipCode).repeatNum + 1;
					futureData.list = map.get(tmpData.zipCode).list;
				}
				
				//update record
				futureData.addNum(data.transAmt);
				futureData.percent_res = futureData.findPercentile();
				record.put(data.keyId, data);
				map.put(tmpData.zipCode, futureData);
				out.println(futureData.toString());
			}
		}
	}
	
	public static void main(String[] args) {
		String inputFile = args[0];
		String percentFile = args[1];
		String outputFile = args[2];
		DataUtil dUtil = new DataUtil();
		dUtil.readIn(inputFile, percentFile, outputFile);
	}
}
