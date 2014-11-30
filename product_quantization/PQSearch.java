import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;
/*
 * This code is for PQ search
 * Author: Fujun Liu Nov. 29 2014
 */

public class PQSearch {
	private ArrayList<ArrayList<Integer>> codes = new ArrayList<ArrayList<Integer>>();
	private HashMap<String, ArrayList<Double>> centers = new HashMap<String, ArrayList<Double>>();
	private ArrayList<String> imgIDs = new ArrayList<String>();
	private int nData, nGroup, nCluster;
	
	private class DataItem{
		private String id;
		private double dist;
		DataItem(String id, double dist){
			this.id = id;
			this.dist = dist;
		}
	}
	private class DataItemComparator implements Comparator<DataItem>{
	    	public int compare(DataItem n1, DataItem n2){
	    		return new Double(n2.dist).compareTo(n1.dist);
	    	}
	}
	 
	/*PQSearch(ArrayList<ArrayList<Integer>> codes, HashMap<String, ArrayList<Double>> centers, ArrayList<String> imgIDs, int nCluster){
		this.imgIDs = imgIDs;
		this.nData = codes.size();
		this.nGroup = codes.get(0).size();
		this.codes = codes;
		
		this.nCluster = nCluster;
		this.centers = centers;
	}*/
	
	PQSearch(ArrayList<String> centerPaths, ArrayList<String> codePaths, int nCluster) throws Exception{
		FileParse.centerParser(centerPaths, centers);
		FileParse.codeParser(codePaths, imgIDs, codes);
		
		nData = codes.size();
		nGroup = codes.get(0).size();
		this.nCluster = nCluster;
	}
	public ArrayList<String> topKSearch(ArrayList<Double> feat, int k){
		int featlen = feat.size();
		double[] featarry = new double[featlen];
		for (int i = 0; i < featlen; ++ i){
			featarry[i] = feat.get(i);
		}
		return topKSearch(featarry, k);
		
	}
	public ArrayList<String> topKSearch(double[] feat, int k){
		k = k + 1; // in case the first item is itself
		
		// compute distance to all centers
		double[][] dist = new double[nGroup][nCluster];
		
		int startIndex = 0;
		for (int gid = 0; gid < this.nGroup; ++ gid){
			// figure out the cluster length
			int len = 0;
			for (int cid = 0; cid < nCluster; ++ cid){
				String tmp = gid + "-" + cid;
				if (centers.containsKey(tmp)){
					len = centers.get(tmp).size();
					break;
				}
			}
			
			double[] subfeat = new double[len];
			System.arraycopy(feat, startIndex, subfeat, 0, len);
			startIndex += len;
			
			for (int cid = 0; cid < nCluster; ++ cid){
				String subcentID = gid + "-" + cid;
				if (!centers.containsKey(subcentID)){
					continue;
				}
				ArrayList<Double> subcent = centers.get(subcentID);
				
				
				dist[gid][cid] = .0;
				for (int i = 0; i < len; ++ i){
					dist[gid][cid] += Math.pow(subfeat[i] - subcent.get(i), 2);
				}
			}
			
		}
		
		// top k query with priority queue
		PriorityQueue<DataItem> minheap = new PriorityQueue<DataItem>(k, new DataItemComparator());
		for (int i = 0; i < nData; ++ i){
			double tmp = .0;
			for (int gid = 0; gid < nGroup; ++ gid){
				//ArrayList<Integer> code = codes.get(i);
				tmp += dist[gid][codes.get(i).get(gid)];
			}
			DataItem item = new DataItem(imgIDs.get(i), tmp);
			if (minheap.size() < k){
				minheap.add(item);
			}else{
				DataItem head = minheap.peek();
				if (item.dist < head.dist){// remove head
					minheap.remove();
					minheap.add(item);
				}
			}
		}
		DataItem[] topret = minheap.toArray(new DataItem[0]);
		Arrays.sort(topret, new DataItemComparator());
		ArrayList<String> ret = new ArrayList<String>();
		for (int i = topret.length - 1; i >= 0; -- i){
			ret.add(topret[i].id);
		}
		return ret;
	}
	
}
