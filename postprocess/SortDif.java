import java.io.BufferedReader; 
import java.io.File;
import java.io.FileInputStream; 
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;  
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class SortDif {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
			ArrayList<Float> a = new ArrayList<Float>();
			HashMap<Float, String> hash = new HashMap<Float, String>(); 
			OutputStreamWriter osw= null;
			InputStreamReader fr = null;
			BufferedReader br = null;
			File f = new File("C:/Users/ibm/Desktop/Homework/CS236Database Management Systems/CS236_F15_Project/2009/result_perstate.txt");
			try
			{
				
				fr = new InputStreamReader(new FileInputStream(f));
				br = new BufferedReader(fr);
				String rec =null;
				String[] argsArr = null;
				while((rec= br.readLine())!=null)
				{
						argsArr = rec.split("\\s+");
						if(argsArr.length==7)
						{
							a.add(Float.parseFloat(argsArr[2]));
							hash.put(Float.parseFloat(argsArr[2]),"State:"+argsArr[0]+" Difference: "+argsArr[2]+" Highest "+ argsArr[3]+","+argsArr[4]+" Lowest "+argsArr[5]+","+argsArr[6]);
						}
						//				osw.write(argsArr[i] +"\t"+"\t");
									
						//System.out.println(); 
					//	osw.write("\n");
					
					 
					
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
				//	if(osw!=null)
				//		osw.close();
				}catch(IOException ex){
					ex.printStackTrace();
				}
			}
			
			try{
				Collections.sort(a);
				osw = new OutputStreamWriter(new FileOutputStream("C:/Users/ibm/Desktop/Homework/CS236Database Management Systems/CS236_F15_Project/2009/result_sortdif.txt",false));
				for(int i =0;i<a.size();i++)
				{
					osw.write(hash.get(a.get(i)));
					osw.write("\n");
				}
				osw.close();
			}catch(IOException ex){
				ex.printStackTrace();
			}
			
		
	}

}
