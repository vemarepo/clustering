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



public class GetSampleClusterDist extends EvalFunc<Double> {
        @Override
        public Double exec(Tuple input) throws IOException {

            DataBag inpbag = (DataBag)input.get(0);
            int feat_dim = (Integer) input.get(1);
            int offset = 3;
            Double output = 0.0;
            System.out.println("Bag size " +  inpbag.size());
            System.out.println("Input size " +  input.size());
            output = getSample(inpbag, feat_dim, offset);
            return output;
           }


        private Double getSample(DataBag inpbag, int feat_dim, int offset) throws ExecException {
            Iterator it = inpbag.iterator();
            Tuple t = (Tuple) it.next();
             System.out.println("Tuple size " +  t.size());
            int cnt = 0;

            double [] sampleArr = new double[feat_dim];
            for(int i =0; i< feat_dim; i++){
                sampleArr[i] = (double) t.get(offset+i) ;
            }

           Tuple clust = (Tuple) t.get(offset+feat_dim+1);
           double [] clustArr = new double[feat_dim];
           for (int i=0; i<feat_dim; i++) {
                clustArr[i] = (double) clust.get(i);
           }

           double dist = 0.0; 
           for (int i=0; i<feat_dim; i++) {                                                                                                                                               
                dist += (clustArr[i] - sampleArr[i])* (clustArr[i] - sampleArr[i]);                                                                                                       
           }                                                                                                                                                                              
                                                                                                                                                                                          
           return dist;                                                                                                                                                                   
        }                                                                                                                                                                                 
                                                                                                                                                                                          
}                                                                                                                                                                                         
                                                                                                                                                                                          
                                                                                                                                                                                          58,0-1        Bot
