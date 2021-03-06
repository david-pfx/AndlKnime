/*******************************************************************************
 * Andl Extended Relational Algebra Nodes for Knime
 * 
 * Andl is A New Data Language. See andl.org.
 *  
 * Copyright (c) David M. Bennett 2020 as an unpublished work.
 *  
 * Rights to copy, modify and distribute this work are granted under the terms of a licence agreement.
 * See readme.md for details.
 *  
 *******************************************************************************/

package org.andl.ra.union;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import org.andl.ra.RaTuple;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Implements the model for the "RaSet" node.
 *
 * The following set operations are supported.
 * 
 * Union merges two tables and removes duplicates
 * Minus outputs rows in the first table but not in the second
 * Intersect outputs rows found in both tables
 * Difference outputs rows in the first table or the second but not both
 * 
 * @author andl
 */
public class RaUnionNodeModel extends NodeModel {
    
	private static final NodeLogger LOGGER = NodeLogger.getLogger(RaUnionNodeModel.class);
	private static final String KEY_SET_OPERATION = "set-operation";
	private static final String DEFAULT_SET_OPERATION = "Union";
	static final String[] ALL_OPERATIONS = {
		"Union", "Minus", "Intersect", "Difference"
	};
	
	private final SettingsModelString _setOperationSettings = createSettingsModel();

	static SettingsModelString createSettingsModel() {
		return new SettingsModelString(KEY_SET_OPERATION, DEFAULT_SET_OPERATION);
	}
	
    //--------------------------------------------------------------------------
    // ctor and dummy overrides
	protected RaUnionNodeModel() {
		super(2, 1);
	}

	/** {@inheritDoc} */
	@Override
	protected void reset() { }
	
	/** {@inheritDoc} */
	@Override
	protected void loadInternals(File nodeInternDir, ExecutionMonitor exec)
	throws IOException, CanceledExecutionException { }

	/** {@inheritDoc} */
	@Override
	protected void saveInternals(File nodeInternDir, ExecutionMonitor exec)
	throws IOException, CanceledExecutionException { }

    //--------------------------------------------------------------------------
    // execute, configure, settings
    //
    // union creates a new table using a container
    //
	
	/** {@inheritDoc} */
	@Override
	protected BufferedDataTable[] execute(final BufferedDataTable[] inData, final ExecutionContext exec)
	throws Exception {

		String operation = _setOperationSettings.getStringValue();
		LOGGER.debug("Begin setop=" + operation);

		outputGenerator outgen = new outputGenerator(exec, inData[0].getDataTableSpec());
		return new BufferedDataTable[] { 
			outgen.getOutputTable(inData, operation)
		};
	}

	/** {@inheritDoc} */
	@Override
	protected DataTableSpec[] configure(final DataTableSpec[] inSpecs) throws InvalidSettingsException {
		String operation = _setOperationSettings.getStringValue();
		if (!(Arrays.asList(ALL_OPERATIONS).contains(operation)))
			throw new InvalidSettingsException("The selected operation is not valid: '" + operation + "'");
		
		return new DataTableSpec[] { 
			createOutputSpec(inSpecs[0]) 
		};
	}

	private DataTableSpec createOutputSpec(DataTableSpec inSpec) {
		return inSpec;
	}

	/** {@inheritDoc} */
	@Override
	protected void saveSettingsTo(final NodeSettingsWO settings) {
		_setOperationSettings.saveSettingsTo(settings);
	}

	/** {@inheritDoc} */
	@Override
	protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
		_setOperationSettings.loadSettingsFrom(settings);
	}

	/** {@inheritDoc} */
	@Override
	protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		_setOperationSettings.validateSettings(settings);
	}
}

//==========================================================================
//
// Implement the algorithms to combine two inputs to one output by a Set operation
//
class outputGenerator {
	final ExecutionContext _exec;
	DataTableSpec _outputSpec;	
	BufferedDataContainer _container;
	long _insize = 0;
	int _incounter = 0;
	int _outcounter = 0;
	
	outputGenerator(ExecutionContext exec, DataTableSpec outputSpec) {
		_exec = exec;
		_outputSpec = outputSpec;
	}

	BufferedDataTable getOutputTable(final BufferedDataTable[] inData, String operation)
	throws Exception {
		String[] allops = RaUnionNodeModel.ALL_OPERATIONS;
		return 
			  allops[0].equals(operation) ? getUnion(inData)
			: allops[1].equals(operation) ? getMinus(inData)
			: allops[2].equals(operation) ? getIntersect(inData)
			: allops[3].equals(operation) ? getDifference(inData)
			: null;
	}

	// return the Set Union of the two input tables
	BufferedDataTable getUnion(BufferedDataTable[] inputs) throws Exception {
		_container = _exec.createDataContainer(_outputSpec);
		HashSet<RaTuple> hashset = new HashSet<>();
		_insize = inputs[0].size() + inputs[1].size();
		
		// copy all rows to the output, removing duplicates
		for (BufferedDataTable input : inputs) {
			for (CloseableRowIterator iter = input.iterator(); iter.hasNext(); ) {
				++_incounter;
				DataRow row = iter.next();
				RaTuple tuple = new RaTuple(row);
				if (!(hashset.contains(tuple))) {
					addRow(row);
					hashset.add(tuple);
				}
			}
		}
		_container.close();
		return _container.getTable();
	}
	
	// return the Set Minus of the two input tables
	BufferedDataTable getMinus(BufferedDataTable[] inputs) throws Exception {
		_container = _exec.createDataContainer(_outputSpec);
		HashSet<RaTuple> hashset = new HashSet<>();
		_insize = inputs[0].size() + inputs[1].size();

		// add all the right table rows to the hashset
		for (CloseableRowIterator iter = inputs[1].iterator(); iter.hasNext(); ) {
			++_incounter;
			DataRow row = iter.next();
			RaTuple tuple = new RaTuple(row);
			hashset.add(tuple);
		}
		
		// copy left table rows to the output, removing right rows 
		// add to hashset to remove duplicates
		for (CloseableRowIterator iter = inputs[0].iterator(); iter.hasNext(); ) {
			++_incounter;
			DataRow row = iter.next();
			RaTuple tuple = new RaTuple(row);
			if (!(hashset.contains(tuple))) {
				addRow(row);
				hashset.add(tuple);
			}
		}
		_container.close();
		return _container.getTable();
	}
	
	// return the Set Intersection of the two input tables
	BufferedDataTable getIntersect(BufferedDataTable[] inputs) throws Exception {
		_container = _exec.createDataContainer(_outputSpec);
		HashSet<RaTuple> hashset = new HashSet<>();
		_insize = inputs[0].size() + inputs[1].size();

		// add all the right table rows to the hashset
		for (CloseableRowIterator iter = inputs[1].iterator(); iter.hasNext(); ) {
			++_incounter;
			DataRow row = iter.next();
			RaTuple tuple = new RaTuple(row);
			hashset.add(tuple);
		}
		
		// copy left table rows to the output if found in hashset
		// remove from hashset so no duplicates
		for (CloseableRowIterator iter = inputs[0].iterator(); iter.hasNext(); ) {
			++_incounter;
			DataRow row = iter.next();
			RaTuple tuple = new RaTuple(row);
			if (hashset.contains(tuple)) {
				addRow(row);
				hashset.remove(tuple);
			}
		}
		_container.close();
		return _container.getTable();
	}
	
	// return the Set Symmetrical Difference of the two input tables
	// this way takes two passes through the right table. Could do better by marking
	// tuples in the hashset.
	BufferedDataTable getDifference(BufferedDataTable[] inputs) throws Exception {
		_container = _exec.createDataContainer(_outputSpec);
		// two hashsets, one for each side
		HashSet<RaTuple> hashset0 = new HashSet<>();
		HashSet<RaTuple> hashset1 = new HashSet<>();
		_insize = inputs[0].size() + 2 * inputs[1].size();

		// add all the right table rows to the hashset
		for (CloseableRowIterator iter = inputs[1].iterator(); iter.hasNext(); ) {
			++_incounter;
			DataRow row = iter.next();
			RaTuple tuple = new RaTuple(row);
			hashset1.add(tuple);
		}
		
		// copy left table rows to the output if not found in hashset
		// add to hashset so no duplicates
		for (CloseableRowIterator iter = inputs[0].iterator(); iter.hasNext(); ) {
			++_incounter;
			DataRow row = iter.next();
			RaTuple tuple = new RaTuple(row);
			hashset0.add(tuple);
			if (!hashset1.contains(tuple)) {
				addRow(row);
				hashset1.add(tuple);
			}
		}

		// copy right table rows to the output if not found in hashset
		// add to hashset so no duplicates
		for (CloseableRowIterator iter = inputs[1].iterator(); iter.hasNext(); ) {
			++_incounter;
			DataRow row = iter.next();
			RaTuple tuple = new RaTuple(row);
			if (!hashset0.contains(tuple)) {
				addRow(row);
				hashset0.add(tuple);
			}
		}
	
		_container.close();
		return _container.getTable();
	}
	
	// utility function to add a row and check for cancellation
	void addRow(DataRow row) throws Exception {
		DataRow newrow = new DefaultRow("Row" + _outcounter++, row);
		_container.addRowToTable(newrow);
		_exec.checkCanceled();
		_exec.setProgress(_incounter / _insize, "Input row " + _incounter);
	}
	
}

