
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
/*
 * This is used to evaluate the algorithm
 * Author: Fujun Liu Nov. 29 2014
 */

public class TestPQSearchBatch {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//String featDim = otherArgs[6], nGroup = otherArgs[7], nCluster = otherArgs[8], topk = otherArgs[9];
		/*String inputPath = args[1], labelPath =  args[2];
		String centerPath = args[3], codePath = args[4];
		int nCluster = Integer.parseInt(args[5]);
		int topk = Integer.parseInt(args[6]);*/
		
		// hist feature
		//String inputPath = "small_annu_feat_240.txt", labelPath =  "small_annu_label.txt";
		//String centerPath = "matlab/centers-hist.txt", codePath = "matlab/codes-hist";
		//String outputPath = "matlab/pr-hist.txt";
		// cnn feature
		//String inputPath = "cnn_feat_500.txt", labelPath =  "cnn_label.txt";
		//String centerPath = "matlab/centers-cnn.txt", codePath = "matlab/codes-cnn";
		//String outputPath = "matlab/pr-cnn.txt";
		// hadoopcnn feature
		String inputPath = "cnn_feat_500.txt", labelPath =  "cnn_label.txt";
		String centerPath = "matlab/centers-hadoopcnn", codePath = "matlab/codes-hadoopcnn";
		String outputPath = "matlab/pr-hadoopcnn.txt";
		
		// llc feature
		//String inputPath = "llc_feat_21504.txt", labelPath =  "llc_label.txt";
		//String centerPath = "matlab/centers-llc.txt", codePath = "matlab/codes-llc";
		//String outputPath = "matlab/pr-llc.txt";
		
		int nCluster = 256;
		int nClass1 = 2730;
		int nClass2 = 2715;
		
		ArrayList<String> centerPaths = new ArrayList<String>();
		ArrayList<String> codePaths = new ArrayList<String>();
		centerPaths.add(centerPath);
		codePaths.add(codePath);
		// build pq
		PQSearch pq = new PQSearch(centerPaths, codePaths, nCluster);
		// read image labels
		HashMap<String, String> labels = new HashMap<String, String>();
		{
			BufferedReader br = new BufferedReader(new FileReader(labelPath));
			String line = null;
			while ((line = br.readLine()) != null){
				String[] tmp = line.split("\\s+");
				labels.put(tmp[0], tmp[1]);
			}
		}
		
		// read line
		
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath)));
		{
			int maxtopk = 5000;
			int topk = maxtopk;
			ArrayList<String> imgIDs = new ArrayList<String>();
			ArrayList<ArrayList<String>> allnnlist = new ArrayList<ArrayList<String>>();
			BufferedReader br = new BufferedReader(new FileReader(inputPath));
			String line = null;
			
			while ((line = br.readLine()) != null){
				String[] tmp = line.split("\\s+");
				String imgID = tmp[0];
				double[] feat = new double[tmp.length - 1];
				for (int i = 0; i < feat.length; ++ i){
					feat[i] = Double.parseDouble(tmp[i+1]);
				}
				ArrayList<String> nnlist = pq.topKSearch(feat, topk);
				
				imgIDs.add(imgID);
				allnnlist.add(nnlist);
			}
			br.close();
			
		    topk = 10;
		    int nImgs = imgIDs.size();
			while (topk <= maxtopk){
				double acc = 0;
				double recall = 0;
				int rightclassify = 0;
				
				for (int ii = 0; ii < nImgs; ++ ii){
					int rights = 0;
					String imgID = imgIDs.get(ii);
					for (int j = 0; j <= topk; ++ j){
						if (!imgID.equals(allnnlist.get(ii).get(j))){
							if (labels.get(imgID).equals(labels.get(allnnlist.get(ii).get(j)))){
								++ rights;
							}
						}
					}
					double ratio = 1.0*rights/topk;
					acc += ratio;
					if (ratio > 0.5){// majority voting for classification
						++ rightclassify;
					}
					if (labels.equals("1")){
						recall += 1.0*rights/nClass1;
					}else{
						recall += 1.0*rights/nClass2;
					}
				}
				
				acc /= nImgs;
				recall /= nImgs;
				double classifyacc = (double)rightclassify/nImgs;
				String ret = topk + "  " + acc + "  " + recall + "  " + classifyacc;
				System.out.println(ret);
				bw.write(ret);
				bw.write("\n");
				
				if (topk < 100){
					topk += 10;
				}else{
					topk += 100;
				}
			}
			
			//System.out.println("All: " + all + ", right: " + rights);
		}
		bw.close();
	}

}

