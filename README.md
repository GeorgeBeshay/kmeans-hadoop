## Description
This project implements the K-Means clustering algorithm using Hadoop's 
MapReduce framework for parallel computation. It iteratively computes 
centroids by assigning data points to the nearest centroid and calculating 
the average of points in each cluster. The process is split into mapper, 
combiner, and reducer tasks, where the first two run locally on each data node, 
for efficient distributed computation. The `Driver` class manages the overall 
workflow, handling centroid computation and data labeling. The program takes as 
input the dataset, desired number of centroids, and output paths for centroids 
and labeled data. This implementation is designed for scalable, large-scale 
clustering on Hadoop.

## Design
![image](https://github.com/user-attachments/assets/3f81d52a-5e47-44bf-bc40-b3899ee7cd9b)

![image](https://github.com/user-attachments/assets/90eee4d0-d7b9-42be-a457-366449d235d5)
