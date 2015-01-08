package myudfs;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.util.Iterator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;

public class GetClusterId extends EvalFunc<Integer> {
        @Override
 	public Integer exec(Tuple input) throws IOException {
            DataBag inpbag = (DataBag)input.get(0);
            DataBag clusterbag = (DataBag)input.get(1);
            int feat_dim = (Integer) input.get(2);
            int num_clusters = (Integer) input.get(3);
            Integer N = getClosestCluster(inpbag, clusterbag, feat_dim, num_clusters);
            return N; 
        }

	private double [] getSample(DataBag inpbag, int feat_dim) throws ExecException {
            double []sample = new double [feat_dim];
	    Iterator it = inpbag.iterator();
            if (it != null && it.hasNext()) {
                Tuple t = (Tuple) it.next();
                for (int i=0; i<feat_dim; i++) {
                    sample[i] = (Double)t.get(i+1);
                }
            }
            return sample;
	}

        private double[][] getCluster(DataBag clusterbar, int feat_dim, int num_clusters) throws ExecException {
            double[][] clusters = new double [num_clusters][feat_dim];
	    Iterator it = clusterbar.iterator();
	    int r = 0;
            while(it.hasNext()){
		Tuple t = (Tuple) it.next();
		for (int i =0;i<feat_dim; i++) {
			clusters[r][i] = (Double) t.get(i+1); 
		} 
		r += 1;
      	    }
	    return clusters;
        } 

	private int getClosestCluster(DataBag inpbag, DataBag clusterbag, int feat_dim, int num_clusters) throws ExecException {
            double[]sample = getSample(inpbag, feat_dim);
            double[][]clusters = getCluster(clusterbag, feat_dim, num_clusters);
            int closest_idx = 0;
            double min_dist = 1e12;
            double dist =0.0;
            for (int i=0; i < num_clusters; i++) {
                    dist = 0;
                    for (int j = 0; j<feat_dim; j++) {
                            dist += (clusters[i][j] - sample[j])*(clusters[i][j] - sample[j]);
                    }

                    if( dist < min_dist) {
                            closest_idx = i;
                            min_dist = dist;
                    }

            }
            return closest_idx;	
	}
 
}
