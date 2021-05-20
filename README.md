# Spatial-Data-Analysis
Spatial Data Analysis using Apache Spark and HDFS


Nowadays data collection is performed in an increasingly automated way. 
Global spatial data of multinational companies, such as Google, are gathered from satellites, planes and special vehicles equipped with cameras and GPS.


How to query and analyze such large spatial data with is a fundamental challenge. 
Systems like Apache Spark have witnessed great success in big data processing by using distributed memory storage and computing.

Problem Statement:
Let P = {p0, p1, · · · , pn−1} and Q = {q0, q1, · · · , qm−1} be two set of points in E, and a distance threshold ε ∈ R.
Then, the result of the εDistance Join query is the set, εDJ(P, Q, ε) ⊆ P×Q, containing all the possible different pairs
of points from P × Q that have a distance of each other smaller than, or equal to ε:
εDJ(P, Q, ε) = {(pi, qj) ∈ P × Q : dist(pi, qj) ≤ ε}

# Spatial Index with KD-TREE ALGORITHM

![image](https://user-images.githubusercontent.com/74420150/119017195-69287900-b9a3-11eb-9d6a-49348c027190.png)

# Brute Force Distance Join Results

![image](https://user-images.githubusercontent.com/74420150/119018361-ab05ef00-b9a4-11eb-9146-d912b53f9171.png)


# Distance Join Algorithm with KD-TREE ALGORITHM
![image](https://user-images.githubusercontent.com/74420150/119019094-7ba3b200-b9a5-11eb-8505-f2e5e4d974a0.png)
