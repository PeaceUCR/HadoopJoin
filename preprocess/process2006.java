import java.io.BufferedReader; 
import java.io.File;
import java.io.FileInputStream; 
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;  
import java.io.OutputStreamWriter;
import java.util.ArrayList;

public class process2006 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
			OutputStreamWriter osw= null;
			InputStreamReader fr = null;
			BufferedReader br = null;
			File f = new File("C:/Users/ibm/Desktop/Homework/CS236Database Management Systems/CS236_F15_Project/2006.txt");
			try
			{
				osw = new OutputStreamWriter(new FileOutputStream("C:/Users/ibm/Desktop/Homework/CS236Database Management Systems/CS236_F15_Project/2006/u2006_process.txt",false));
				fr = new InputStreamReader(new FileInputStream(f));
				br = new BufferedReader(fr);
				String rec =null;
				String[] argsArr = null;
				while((rec= br.readLine())!=null)
				{
						argsArr = rec.split("\\s+");
						for (int i = 0; i < argsArr.length; i++) 
						{ 
								if(i==0||i==1||i==2||i==3)
									{
									//String s = argsArr[i].replace("\"","");
						//			System.out.print(s +"\t");
										osw.write(argsArr[i] +"\t"+"\t");
									}
						}
						//System.out.println(); 
						osw.write("\n");
					
					 
					
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
	

}
