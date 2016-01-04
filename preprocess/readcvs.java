import java.io.BufferedReader; 
import java.io.File;
import java.io.FileInputStream; 
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;  
import java.io.OutputStreamWriter;
import java.util.ArrayList;

public class readcvs {
	 
	public static void main(String[] args)
	{	

		OutputStreamWriter osw= null;
		InputStreamReader fr = null;
		BufferedReader br = null;
		File cvsf = new File("C:/Users/ibm/Desktop/Homework/CS236Database Management Systems/CS236_F15_Project/WeatherStationLocations.csv");
		try
		{
			osw = new OutputStreamWriter(new FileOutputStream("C:/Users/ibm/Desktop/Homework/CS236Database Management Systems/CS236_F15_Project/location_process_noST.txt",false));
			fr = new InputStreamReader(new FileInputStream(cvsf));
			br = new BufferedReader(fr);
			String rec =null;
			String[] argsArr = null;
			while((rec= br.readLine())!=null)
			{
				argsArr = rec.split(",");
				if(argsArr[3].equals("\"US\"")&&argsArr[4].equals("\"\""))//retain CTRY=US
				{
					for (int i = 0; i < argsArr.length; i++) { 
							if(i==0||i==1||i==3||i==4)
								{
									String s = argsArr[i].replace("\"","");
					//			System.out.print(s +"\t");
									osw.write(s +"\t"+"\t");
								}
							}
					//System.out.println(); 
					osw.write("\n");
				}
				 
				
			}
		}catch(IOException e)
		{
			e.printStackTrace();
		}finally{
			
			try{
				if(fr!=null)
					fr.close();
				if(br!=null)
					br.close();
				if(osw!=null)
					osw.close();
			}catch(IOException ex){
				ex.printStackTrace();
			}
		}
	}
	/*
	public class LocationData
	{
		public String USAF;
		public String WBAN;
		public String STATION;
		public String CTRY;
		public String STATE;
		public String LAT;
		public String LON;
		public String ELEV;
		public String BEGIN;
		public String END;
		public LocationData(String uSAF, String wBAN, String sTATION,
				String cTRY, String sTATE, String lAT, String lON, String eLEV,
				String bEGIN, String eND) {
			super();
			USAF = uSAF;
			WBAN = wBAN;
			STATION = sTATION;
			CTRY = cTRY;
			STATE = sTATE;
			LAT = lAT;
			LON = lON;
			ELEV = eLEV;
			BEGIN = bEGIN;
			END = eND;
		}
		
		
		*/
}
	
