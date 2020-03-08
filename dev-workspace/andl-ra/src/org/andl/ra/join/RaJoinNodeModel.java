package org.andl.ra.join;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Stream;

import org.andl.ra.RaTuple;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
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
 * This is an example implementation of the node model of the
 * "RaJoin" node.
 * 
 * The following join operations are supported. All joins are based on common 
 * columns matched by name and type.
 * 
 * Join is the natural join. The output includes all columns from both inputs.
 * Compose is Join with the join columns removed from the output.
 * Semijoin is the left (top) input with only rows that DO match the right (bottom) input included.
 * Antijoin is the left (top) input with only rows that DO NOT match the right (bottom) input included.
 * 
 * @author andl
 */
public class RaJoinNodeModel extends NodeModel {
    
	private static final NodeLogger LOGGER = NodeLogger.getLogger(RaJoinNodeModel.class);
	private static final String KEY_OPERATION = "join-operation";
	private static final String DEFAULT_OPERATION = "Join";
	static final String[] ALL_OPERATIONS = {
		"Join", "Semijoin", "Antijoin"
	};
	private final SettingsModelString _joinOperationSettings = createSettingsModel();

	/**
	 * Constructor for the node model.
	 */
	protected RaJoinNodeModel() {
		super(2, 1);
	}

	/**
	 * @return a new SettingsModelString with the key for the set operation String
	 */
	static SettingsModelString createSettingsModel() {
		return new SettingsModelString(KEY_OPERATION, DEFAULT_OPERATION);
	}

	/** {@inheritDoc} */
	@Override
	protected BufferedDataTable[] execute(final BufferedDataTable[] inData, final ExecutionContext exec)
	throws Exception {

		String operation = _joinOperationSettings.getStringValue();
		LOGGER.debug("Begin setop=" + operation);

		outputGenerator outgen = new outputGenerator(exec, inData);
		BufferedDataTable out = 
				operation == ALL_OPERATIONS[0] ? outgen.getJoin(inData)
				: operation == ALL_OPERATIONS[1] ? outgen.getSemijoin(inData, true)
				: operation == ALL_OPERATIONS[2] ? outgen.getSemijoin(inData, false)
				: null;
		return new BufferedDataTable[] { out };
	}
	
	/** {@inheritDoc} */
	@Override
	protected DataTableSpec[] configure(final DataTableSpec[] inSpecs) throws InvalidSettingsException {

		String operation = _joinOperationSettings.getStringValue();
		if (!(Arrays.asList(ALL_OPERATIONS).contains(operation)))
			throw new InvalidSettingsException("The selection operation is not valid");

		specGenerator gen = new specGenerator(inSpecs[0], inSpecs[1]);
		
		DataTableSpec outSpec = 
			  operation == ALL_OPERATIONS[0] ? new DataTableSpec(gen._leftInputSpec, gen._rightSpec)
			: operation == ALL_OPERATIONS[1] ? inSpecs[0]
			: operation == ALL_OPERATIONS[2] ? inSpecs[0]
			: null;

		return new DataTableSpec[] { outSpec };
	}

	/** {@inheritDoc} */
	@Override
	protected void saveSettingsTo(final NodeSettingsWO settings) {
		_joinOperationSettings.saveSettingsTo(settings);
	}

	/** {@inheritDoc} */
	@Override
	protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
		_joinOperationSettings.loadSettingsFrom(settings);
	}

	/** {@inheritDoc} */
	@Override
	protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		_joinOperationSettings.validateSettings(settings);
	}

	/** {@inheritDoc} */
	@Override
	protected void loadInternals(File nodeInternDir, ExecutionMonitor exec)
	throws IOException, CanceledExecutionException { }

	/** {@inheritDoc} */
	@Override
	protected void saveInternals(File nodeInternDir, ExecutionMonitor exec)
	throws IOException, CanceledExecutionException { }

	/** {@inheritDoc} */
	@Override
	protected void reset() { }
}

/**
 * Implement the algorithms to combine two inputs to one output by a Join operation
 */
class outputGenerator {
	final ExecutionContext _exec;
	specGenerator _specs;
	BufferedDataContainer _container;
	long _insize = 0;
	int _incounter = 0;
	int _outcounter = 0;
	
	outputGenerator(ExecutionContext exec, BufferedDataTable[] inputs) 
	throws InvalidSettingsException {
		_exec = exec;
		_specs = new specGenerator(inputs[0].getDataTableSpec(), inputs[1].getDataTableSpec());
	}

	// return the Set Union of the two input tables
	BufferedDataTable getJoin(BufferedDataTable[] inputs) throws Exception {
		
		// output table assumes L + J + R
		DataTableSpec outSpec = new DataTableSpec(_specs._leftInputSpec, _specs._rightSpec);
		
		_container = _exec.createDataContainer(outSpec);
		//HashSet<RaTuple> hashset = new HashSet<>();
		_insize = inputs[0].size() + inputs[1].size();
		
		Map<RaTuple, ArrayList<RaTuple>> map = buildIndex(inputs[1], _specs._joinrightcolmap, _specs._rightcolmap);
		for (CloseableRowIterator iter = inputs[0].iterator(); iter.hasNext();) {
			++_incounter;
			DataRow leftrow = iter.next();
			RaTuple jointuple = new RaTuple(leftrow, _specs._joinleftcolmap);
			if (map.containsKey(jointuple)) {
				for (RaTuple righttuple : map.get(jointuple)) {
					// append right-only columns to left row
					DataCell[] newcells = Stream
						.concat(leftrow.stream(), Arrays.asList(righttuple.getCells()).stream())
						.toArray(DataCell[]::new);
					addRow(newcells);
				}
			}
		}
		
		// TODO
		_container.close();
		return _container.getTable();
	}
	
	// return the semi join (or anti join) of the two input tables
	BufferedDataTable getSemijoin(BufferedDataTable[] inputs, boolean matching) throws Exception {
		
		DataTableSpec outSpec = _specs._leftInputSpec;
		_container = _exec.createDataContainer(outSpec);
		HashSet<RaTuple> hashset = new HashSet<>();
		_insize = inputs[0].size() + inputs[1].size();

		// add all the right table join tuples to the hashset
		for (CloseableRowIterator iter = inputs[1].iterator(); iter.hasNext(); ) {
			++_incounter;
			DataRow row = iter.next();
			RaTuple tuple = new RaTuple(row, _specs._joinrightcolmap);
			hashset.add(tuple);
		}
		
		// copy left table rows to the output, remove duplicates
		// matching means if join tuple found in the hashset, else the reverse
		for (CloseableRowIterator iter = inputs[0].iterator(); iter.hasNext();) {
			++_incounter;
			DataRow row = iter.next();
			RaTuple tuple = new RaTuple(row, _specs._joinleftcolmap);
			if (hashset.contains(tuple)) {
				if (matching) {
					addRow(row);
					hashset.remove(tuple);
				}
			} else if (!matching) {
				addRow(row);
				hashset.add(tuple);
			}
		}
		_container.close();
		return _container.getTable();
	}
	
	// build an index from join tuple to tuple of remaining columns
	private Map<RaTuple, ArrayList<RaTuple>> buildIndex(BufferedDataTable input, int[] keycolmap, int[] rowcolmap) 
	throws CanceledExecutionException {
		Map<RaTuple, ArrayList<RaTuple>> map = new HashMap<>();
		for (CloseableRowIterator iter = input.iterator(); iter.hasNext();) {
			DataRow row = iter.next();
			++_incounter;

			RaTuple key = new RaTuple(row, keycolmap);
			RaTuple value = new RaTuple(row, rowcolmap); 
			if (!map.containsKey(key))
				map.put(key, new ArrayList<>());				
			map.get(key).add(value);

			_exec.checkCanceled();
			_exec.setProgress(_incounter / _insize, "Input row " + _incounter);
		}
		return map;
	}

	private void addRow(DataRow row) throws Exception {
		addRow(new RaTuple(row).getCells());
	}
	
	private void addRow(DataCell[] cells) throws Exception {
		DataRow newrow = new DefaultRow("Row" + _outcounter++, cells);
		_container.addRowToTable(newrow);
		_exec.checkCanceled();
		_exec.setProgress(_incounter / _insize, "Input row " + _incounter);
	}
	
}

/**
 * internal class to compute various column specs and maps
 * 
 * all pre-calculated because they get used on every row
 */
class specGenerator {
	DataTableSpec _leftSpec, _rightSpec, _joinSpec, _leftInputSpec, _rightInputSpec;
	int[] _joinleftcolmap, _joinrightcolmap, _leftcolmap, _rightcolmap;

	specGenerator(DataTableSpec leftInputSpec, DataTableSpec rightInputSpec) 
	throws InvalidSettingsException {
		_leftInputSpec = leftInputSpec;
		_rightInputSpec = rightInputSpec;

		_joinSpec = getJoinSpec(_leftInputSpec, _rightInputSpec);
		_leftSpec = specMinus(_leftInputSpec, _joinSpec);
		_rightSpec = specMinus(_rightInputSpec, _joinSpec);
		
		_leftcolmap = getColMap(_leftSpec, _leftInputSpec);
		_joinleftcolmap = getColMap(_joinSpec, _leftInputSpec);
		_rightcolmap = getColMap(_rightSpec, _rightInputSpec);
		_joinrightcolmap = getColMap(_joinSpec, _rightInputSpec);

	}
	
	// get a column map for getting required columns from a row
	private int[] getColMap(DataTableSpec destSpec, DataTableSpec sourceSpec) {
		return destSpec.stream()
			.mapToInt(s -> sourceSpec.findColumnIndex(s.getName()))
			.toArray();
	}

	private DataTableSpec specMinus(DataTableSpec leftSpec, DataTableSpec rightSpec) {
		DataColumnSpec[] cols = leftSpec.stream()
			.filter(s -> !rightSpec.containsName(s.getName()))
			.toArray(DataColumnSpec[]::new);
		return new DataTableSpec(cols);
	}

	private DataTableSpec getJoinSpec(DataTableSpec leftSpec, DataTableSpec rightSpec) 
	throws InvalidSettingsException {
		DataColumnSpec[] cols = leftSpec.stream()
			.filter(s -> rightSpec.containsName(s.getName()))
			.toArray(DataColumnSpec[]::new);
		for (DataColumnSpec col : cols) {
			DataColumnSpec match = rightSpec.getColumnSpec(col.getName());
			if (match != null && match.getType() != col.getType())
				throw new InvalidSettingsException("Join columns not same type: " + col.getName());
		}
		return new DataTableSpec(cols);
	}

	
}

