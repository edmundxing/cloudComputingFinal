import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;


public class FileParse {
	
	public static void centerParser(ArrayList<String> paths, HashMap<String, ArrayList<Double>> centers) throws Exception{
		if (centers == null){
			centers = new HashMap<String, ArrayList<Double>>();
		}
		
		if (paths != null && paths.size() > 0){
			
			for (String path:paths){
				BufferedReader fBuffer = new BufferedReader(new FileReader(path));
				String line = null;
				while ((line = fBuffer.readLine()) != null){
					String[] cluscent = line.split("\\s+");
					
					String subcentID = cluscent[0];
				
					ArrayList<Double> subcent = new ArrayList<Double>();
					for (int i = 1; i < cluscent.length; ++ i){
						subcent.add(Double.parseDouble(cluscent[i]));
					}
					centers.put(subcentID, subcent);
				}
			}
		}
	}
	
	public static void codeParser(ArrayList<String> paths, ArrayList<String> imgIDs, ArrayList<ArrayList<Integer> > codes) throws Exception{
		if (imgIDs == null){
			imgIDs = new ArrayList<String>();
		}
		if (codes == null){
			codes = new ArrayList<ArrayList<Integer>>();
		}
		
		if (paths != null && paths.size() > 0){
			
			for (String path:paths){
				BufferedReader fBuffer = new BufferedReader(new FileReader(path));
				String line = null;
				while ((line = fBuffer.readLine()) != null){
					String[] strcode = line.split("\\s+");
				    String imgID = strcode[0];
				    ArrayList<Integer> code = new ArrayList<Integer>();
				
					for (int i = 1; i < strcode.length; ++ i){
						code.add(Integer.parseInt(strcode[i]));
					}
					
					imgIDs.add(imgID);
					codes.add(code);
				}
			}
		}
		
	}
}
