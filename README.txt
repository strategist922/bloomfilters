Bloom Filter
------------

This module implements Bloom filters, as defined by Bloom in 1970.

The Bloom filter is a data structure that was introduced in 1970 and that has 
been adopted by the networking research community in the past decade thanks to 
the bandwidth efficiencies that it offers for the transmission of set 
membership information between networked hosts.  

A sender encodes the information into a bit vector, the Bloom filter, that is 
more compact than a conventional representation. Computation and space costs 
for construction are linear in the number of elements.  

The receiver uses the filter to test whether various elements are members of 
the set. Though the filter will occasionally return a false positive, it will 
never return a false negative. When creating the filter, the sender can choose 
its desired point in a trade-off between the false positive rate and the size. 

This implementation comes from:

  European Commission One-Lab Project 034819
  http://www.one-lab.org/


Maven
-----

Once you have installed Maven, you can have fun with the following commands:

  mvn -Declipse.workspace=/opt/workspace eclipse:add-maven-repo
  mvn eclipse:clean eclipse:eclipse -DdownloadSources=true -DdownloadJavadocs=true
  mvn dependency:resolve
  mvn compile
  mvn test
  mvn package
  mvn site
  mvn install
  mvn deploy
  mvn pmd:pmd
  mvn cobertura:cobertura
  mvn findbugs:findbugs
  mvn findbugs:gui 


                                                        -- Talis Platform Team
