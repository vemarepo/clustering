register myudfs;
A  = load '$INPUT' using PigStorage('\t') as (id:int, x_0:double, x_1:double) ;
B  = load '$CLUSTER_INPUT' using PigStorage('\t') as (id:int, x_0:double, x_1:double) ;

--- Assign each step to its cluster (E STEP)
AG = GROUP A by $0;
BG = GROUP B ALL;
C =  FOREACH AG generate group as sid, myudfs.GetClusterId($1, BG.$1 , 2,2 ) as cid;
comb_clustid_sample = JOIN C by sid, A by id;


--- Group the sample points by cluster and estimate new cluster (M STEP)
clust_grp = GROUP comb_clustid_sample by cid;
new_clusters = FOREACH clust_grp generate group as cid, myudfs.GetClusterMeans(comb_clustid_sample, group, 2) as clustmean;
CLUST_OUTP = FOREACH new_clusters generate cid , FLATTEN(clustmean);
STORE CLUST_OUTP into '$CLUSTER_MEAN_OUT';


--- Find Within cluster distance
new_clustid_sample = JOIN comb_clustid_sample by cid, new_clusters by cid;
new_clust_sample_grp = GROUP new_clustid_sample  by $0;
clust_sample_dist  = FOREACH new_clust_sample_grp generate group as sid, myudfs.GetSampleClusterDist($1, 2) as scdist;
dist_grp = GROUP clust_sample_dist ALL;
WITHIN_CLUST_DIST = FOREACH dist_grp GENERATE SUM(clust_sample_dist.scdist);
STORE WITHIN_CLUST_DIST into '$CLUSTER_DIST_OUT';
