# AndlRaKnime adds the Relational Algebra to Knime

Andl is A New Database Language. See <http://andl.org>.

Knime is a visual programming language, which models the flow of data in tables as a graphical workflow, made up of nodes and edges.
To learn more about Knime, go here: <https://www.knime.com/>.

AndlRaKnime provides a set of nodes that implement the Relational Algebra.
Using these nodes allows you to perform SQL-like operations on a wide variety of data sources, such as
CSV files or data retrieved online.

For more about the Relational Algebra see here: <https://en.wikipedia.org/wiki/Relational_algebra>.
The set of nodes is currently:

* Selection (as a Boolean expression)
* Projection
* Join (and semijoin, antijoin)
* Rename
* Union (and Minus, Intersection, Difference)
* New Value (as a JEXL expression)

For more about JEXL see here: <http://commons.apache.org/proper/commons-jexl/>.

Sample workflows are included to demonstrate these capabilities.

## FIRST DO THIS

The release includes a JAR file plugin and a ZIP archive of sample nodes.
Put the JAR in your Knime installation dropins folder.
Unzip the samples into your Knime runtime workspace.
Then start or restart Knime.

A set of Andl nodes will appear in your node repository.
The samples and data will appear in your LOCAL folder.

## LICENCE

This version of AndlRaKnime is free for any kind of experimental use, especially helping to make it better.
For now, the licence does not grant rights for distribution or commercial use.
That will have to wait until I can choose the right licence, which depends a lot on who might want to use it.

AndlRaKnime is built using a variety of components based on Java, Eclipse and Knime.
The reader is referred to those products for relevant licence terms.

Please contact me with any questions or suggestions at david@andl.org.
