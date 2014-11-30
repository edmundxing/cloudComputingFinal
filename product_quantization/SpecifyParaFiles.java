import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;


public class SpecifyParaFiles {
	public static void main(String[] args) throws Exception{
		String paraname = "topkparas.txt";
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(paraname)));
		int maxtopk = 5000;
		int topk = 10;
		while(topk <= maxtopk){
			bw.write(Integer.toString(topk));
			bw.write("\n");
			
			if (topk < 100){
				topk += 10;
			}else{
				topk += 100;
			}
		}
		bw.close();
	}
}
