# Page-Ranking-System-with-MapReduce
Implement Web page ranking system

Algorithm: 

http://www.math.cornell.edu/~mec/Winter2009/RalucaRemus/Lecture3/lecture3.html

Raw Data(ETL from): 

http://www.limfinity.com/ir/

Source Code:

This program iteratively execute 2 MapReduce jobs until result converge: MatrixCellMR and UnitSumMR
The first job break matrix into cells and implement multiplication, the second job sums up fraction for all matrix cells.

Result:

The rank weight of all webpage, higher score means more important 

--------------------------------------------------
How to run Hadoop MapReduce:

 1. Upload input files into hadoop file system:
    
    $ hdfs fs -mkdir /input
    
    $ hdfs fs -put *.txt /input/
    
 2. Generate jar file from your java mapReduce program
    
    $ hadoop com.sun.tools.javac.Main *.java
    
    $ jar cf myprog.jar *.class
 
 3. Run MapReduce on Hadoop
 
    $ hadoop jar myprog.jar [main class] /input /output
 
 4. Check the result from MapReduce
 
    $ hdfs fs -cat /output/part-r-00000
