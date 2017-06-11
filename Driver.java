import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

public class Driver {
	public static void main(String args[]) 
			throws ClassNotFoundException, IOException, InterruptedException {

		String trans = new String(args[0]);
		String pr = new String(args[1]);
		String tmp = new String(args[2]);
		
		for (int i = 0; i < Integer.parseInt(args[4]); ++i) {
			String [] paras1 = {trans, pr + Integer.toString(i), tmp + Integer.toString(i), args[3]};
			MatrixCellMR.main(paras1);
			String [] paras2 = {tmp + Integer.toString(i), pr + Integer.toString(i + 1)};
			UnitSumMR.main(paras2);
			
			// clean up temporary directory
			Configuration conf = new Configuration();
			FileSystem hdfsBuild = FileSystem.get(URI.create("hdfs://hadoop-master:9000"), conf);
			hdfsBuild.delete(new Path(tmp + Integer.toString(i)), true);
		}
	}
}
