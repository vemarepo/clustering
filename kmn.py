!/usr/bin/python 

# explicitly import Pig class 
from org.apache.pig.scripting import Pig
from java.util import Properties

# COMPILE: compile method returns a Pig object that represents the pipeline
Pig.registerJar("./depend/myudfs.jar");
Pig.set("exectype", "local") 

P = Pig.compileFromFile("""run_cluster.pig""")
Q = P.bind({'INPUT':'input_data.txt',
            'CLUSTER_INPUT':'cluster_init/*',
            'CLUSTER_MEAN_OUT':'cluster_iter' , 
            'CLUSTER_DIST_OUT':'cluster_dist', 
            })

# BIND and RUN 
props = Properties()
props.put("exectype", "local")
result = Q.runSingle(props)

if result.isSuccessful() :
    print 'Pig job succeeded'
else :
    raise 'Pig job failed'