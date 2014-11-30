

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.nio.channels.FileChannel;
/*
 This is for query. In the example, I hard-coded all necessary inputs
 Author: Fujun Liu  Nov. 29 2014
 */
public class QueryDemo {
	// copy files
	private static void copyFileUsingFileChannels(File source, File dest) throws IOException {
		FileChannel inputChannel = null;
		FileChannel outputChannel = null;
		try {
			inputChannel = new FileInputStream(source).getChannel();
			outputChannel = new FileOutputStream(dest).getChannel();
			outputChannel.transferFrom(inputChannel, 0, inputChannel.size());
		} finally {
			inputChannel.close();
			outputChannel.close();
		}
	}

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
		String imgRootPath = "/home/fujun/Cloud-Computing/images/all";
		String centerPath = "matlab/centers-hadoopcnn", codePath = "matlab/codes-hadoopcnn";
		ArrayList<String> queryIDs = new ArrayList<String>();
		queryIDs.add("55-6978_06_015");
		queryIDs.add("78-7147_06_006");
		queryIDs.add("33-4532_09_010");
		queryIDs.add("85-7699_04_009");
		
		// llc feature
		//String inputPath = "llc_feat_21504.txt", labelPath =  "llc_label.txt";
		//String centerPath = "matlab/centers-llc.txt", codePath = "matlab/codes-llc";
		//String outputPath = "matlab/pr-llc.txt";
		
		int nCluster = 256;
		//int nClass1 = 2730;
		//int nClass2 = 2715;
		
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
		
		// read feature
		HashMap<String, ArrayList<Double>> queries = new HashMap<String, ArrayList<Double>>();
		BufferedReader br = new BufferedReader(new FileReader(inputPath));
		String line = null;
		
		while ((line = br.readLine()) != null){
			String[] tmp = line.split("\\s+");
			String imgID = tmp[0];
			for (String queryID:queryIDs){
				if (imgID.equals(queryID)){
					ArrayList<Double> feat = new ArrayList<Double>();
					for (int i = 1; i < tmp.length; ++ i){
						feat.add(Double.parseDouble(tmp[i]));
					}
					queries.put(imgID, feat);
					if (queries.size() == queryIDs.size()){
						break;
					}
				}
			}
			
		}
		br.close();
		
		// query
		int topk = 4;
		for (String imgID:queries.keySet()){
			ArrayList<Double> feat = queries.get(imgID);
			// top-k query
			ArrayList<String> nnlist = pq.topKSearch(feat, topk);
			// copy files
			File dir = new File(imgID);
			if (!dir.exists()){
				// create dir
				dir.mkdir();
			}
			// copy all images to this dir
			for (int i = 0; i < nnlist.size(); ++ i){
				String imgName = nnlist.get(i);
				String srcPath = imgRootPath + "/" + imgName + ".tif";
				String imglabel = "";
				if (labels.get(imgName).equals("1")){
					imglabel = "adeno";
				}else{
					imglabel = "squma";
				}
				
				String desPath = dir + "/" + i + "-" + imgName + "(" + imglabel + ").tif";
				copyFileUsingFileChannels(new File(srcPath), new File(desPath));
			}
		}
		
	}

}


