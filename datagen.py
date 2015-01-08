import numpy as np
import scipy.linalg as scl
np.random.seed(30) #Setting seed for reproducibility


num_samples = 1000
feat_dim = 2
num_classes = 2

feat_means = np.zeros((num_classes, feat_dim))
feat_cov= np.zeros((num_classes, feat_dim, feat_dim))
feat_sqrtcov= np.zeros((num_classes, feat_dim, feat_dim))

for i in np.arange(num_classes):
    feat_means[i,:] = np.random.rand(1, feat_dim)*100 - 50
    sgm = np.random.randint(1, 10, size=feat_dim)
    sigma = np.diag(sgm)
    u = np.random.rand(feat_dim , feat_dim)*2 - 1
    correl = u - np.diag(np.diag(u)) + np.eye(feat_dim)
    covmat = np.dot(np.dot(sigma, correl), sigma)
    feat_cov[i,:,:] = covmat 
    feat_sqrtcov[i,:,:] = scl.sqrtm(covmat) 

samples = np.zeros((num_samples, feat_dim))

def wrt2str(i, smp):
    stg = str(i) + "\t" + "\t".join(map(str, smp))
    return stg
    

with open('input_data.txt', 'w') as fh:
    for i in np.arange(num_samples):
        r = np.random.randint(0, feat_dim)
        mn = feat_means[r,:]
        sgmm = feat_sqrtcov[r,:,:]  
        rnd = np.random.randn(feat_dim)
        print mn.shape, sgmm.shape, rnd.shape
        smp = mn + np.dot(sgmm , rnd ).T
        print smp.shape
        samples[i,:] = smp
        print >> fh, wrt2str(i, smp)

import matplotlib.pyplot as plt
import seaborn as sns
f = plt.figure()
plt.plot(samples[:,0], samples[:,1], ".")
plt.xlabel("Dim 1")
plt.ylabel("Dim 2")
plt.title("Clusters of Points")
plt.show()
    



