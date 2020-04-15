# AndlEraKnime adds the Andl Extended Relational Algebra to Knime

Andl is A New Database Language. See <http://andl.org>.

Knime is a visual programming language, which models the flow of data in tables as a graphical workflow, made up of nodes and edges.
To learn more about Knime, go here: <https://www.knime.com/>.

AndlEraKnime provides a set of nodes that implement the Andl Extended Relational Algebra.
Using these nodes allows you to perform SQL-like operations on a wide variety of data sources, such as
CSV files or data retrieved online.

For more about the Relational Algebra see here: <https://en.wikipedia.org/wiki/Relational_algebra>.
The terms table, row and column are used here interchangeably with relation, tuple and attribute.

The set of nodes is:

* Projection (remove some columns)
* Rename (a column)
* Selection (rows that satisfy a Boolean JEXL expression)
* Join two tables (and semijoin, antijoin)
* Union of two tables (and Minus, Intersection, Difference)
* New column value (as a JEXL expression)
* Aggregation of a column (selected functions)

For more about JEXL see: <http://commons.apache.org/proper/commons-jexl/>.

Sample workflows are included to demonstrate these capabilities.

## FIRST DO THIS

The release includes a JAR file plugin and a ZIP archive of sample nodes.
Put the JAR in your Knime installation dropins folder.
Unzip the samples into your Knime runtime workspace.
Then start or restart Knime.

A set of Andl nodes will appear in your node repository.
The samples and data will appear in your LOCAL folder.

Alternatively you can install the Andl ERA nodes from the Community site.

## LICENCE

This software is released as open source for unrestricted personal, experimental or non-commercial use, with the intention to invite contributions that help to make it better. 
A licence is hereby granted for you to use or modify it for these purposes without further restriction.

At your option you may use, modify and redistribute the software under the terms of any version of the GNU General Public Licence, by complying with its terms. This author does not agree with the Preamble, but does not impose any condition as to its use.
 
This software is built using a variety of components based on Java, Eclipse and Knime.
The reader is referred to those products for relevant licence terms.

Please contact me with any questions or suggestions at david@andl.org.
