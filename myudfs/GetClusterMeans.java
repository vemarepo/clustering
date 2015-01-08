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

public class GetClusterMeans extends EvalFunc<Tuple> {
        @Override
        public Tuple exec(Tuple input) throws IOException {
            DataBag clusterbag = (DataBag)input.get(0);

            int clid = (Integer) input.get(1);
            int feat_dim = (Integer) input.get(2);
            int offset = 3;
            long num_samples = clusterbag.size();
            Iterator it = clusterbag.iterator();
            double [] outputArr = new double[feat_dim];

            /* Compute Average */
            TupleFactory tupFactory = TupleFactory.getInstance();
            Tuple output = tupFactory.newTuple();

            while(it.hasNext()){
                Tuple t = (Tuple) it.next();
                for(int i=0; i < feat_dim; i++){
                    outputArr[i] += (Double) t.get(offset + i);
                }
            }
            for(int i=0; i < feat_dim; i++){
                outputArr[i] = outputArr[i]/num_samples;
                System.out.println(" Clid " + clid + " " + i + ":" + outputArr[i]);
                output.append(outputArr[i]);
            }

            return output;
        }
}
