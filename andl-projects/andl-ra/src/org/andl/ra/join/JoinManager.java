package org.andl.ra.join;

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
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;

/*******************************************************************************
 * 
 * Implement the algorithms to combine two inputs to one output by a Join operation
 */
class JoinManager {
	static String[] _allOps = RaJoinNodeModel.ALL_OPERATIONS;
	private static final NodeLogger LOGGER = NodeLogger.getLogger(JoinManager.class);
	
	SpecGenerator _specGen;
	DataTableSpec _tableSpec;
	String _operation;

	DataTableSpec getTableSpec() { return _tableSpec; }

	JoinManager(DataTableSpec[] inSpecs, String operation) 
	throws InvalidSettingsException {

//		_allOps = RaJoinNodeModel.ALL_OPERATIONS;
		if (!(Arrays.asList(_allOps).contains(operation)))
			throw new InvalidSettingsException("The selected operation is not valid: '" + operation + "'");

		_specGen = new SpecGenerator(inSpecs[0], inSpecs[1]);
		_operation = operation;
		_tableSpec = tableSpec();
		LOGGER.info(String.format("Join operation op=%s join spec=%s", operation, _specGen._joinSpec));
	}
	
	// return table spec according to operation requested
	DataTableSpec tableSpec() {
		return _allOps[0].equals(_operation) ? new DataTableSpec(_specGen._leftInputSpec, _specGen._rightSpec)
			: _allOps[1].equals(_operation) ? _specGen._leftInputSpec
			: _allOps[2].equals(_operation) ? _specGen._leftInputSpec
			: null;
	}
	
	// shared by implementations
	ExecutionContext _exec;
	BufferedDataContainer _container;
	long _insize = 0;
	int _incounter = 0;
	int _outcounter = 0;

	BufferedDataTable execute(BufferedDataTable[] inData, ExecutionContext exec) 
	throws Exception {
		_exec = exec;
		return _allOps[0].equals(_operation) ? doFullJoin(inData)
				: _allOps[1].equals(_operation) ? doSemijoin(inData, true)
				: _allOps[2].equals(_operation) ? doSemijoin(inData, false)
			    : null;
	}
	
	// return the Set Union of the two input tables
	BufferedDataTable doFullJoin(BufferedDataTable[] inputs) throws Exception {
		
		// output table assumes L + J + R
		DataTableSpec outSpec = new DataTableSpec(_specGen._leftInputSpec, _specGen._rightSpec);
		
		_container = _exec.createDataContainer(outSpec);
		//HashSet<RaTuple> hashset = new HashSet<>();
		_insize = inputs[0].size() + inputs[1].size();
		
		Map<RaTuple, ArrayList<RaTuple>> map = buildIndex(inputs[1], _specGen._joinrightcolmap, _specGen._rightcolmap);
		for (CloseableRowIterator iter = inputs[0].iterator(); iter.hasNext();) {
			++_incounter;
			DataRow leftrow = iter.next();
			RaTuple jointuple = new RaTuple(leftrow, _specGen._joinleftcolmap);
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
	BufferedDataTable doSemijoin(BufferedDataTable[] inputs, boolean matching) throws Exception {
		
		DataTableSpec outSpec = _specGen._leftInputSpec;
		_container = _exec.createDataContainer(outSpec);
		HashSet<RaTuple> hashset = new HashSet<>();
		_insize = inputs[0].size() + inputs[1].size();

		// add all the right table join tuples to the hashset
		for (CloseableRowIterator iter = inputs[1].iterator(); iter.hasNext(); ) {
			++_incounter;
			DataRow row = iter.next();
			RaTuple tuple = new RaTuple(row, _specGen._joinrightcolmap);
			hashset.add(tuple);
		}
		
		// copy left table rows to the output, remove duplicates
		// matching means if join tuple found in the hashset, else the reverse
		for (CloseableRowIterator iter = inputs[0].iterator(); iter.hasNext();) {
			++_incounter;
			DataRow row = iter.next();
			RaTuple tuple = new RaTuple(row, _specGen._joinleftcolmap);
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

/*******************************************************************************
 * Internal class to compute various column specs and maps.
 * <br>
 * All pre-calculated because they get used on every row
 */
class SpecGenerator {
	DataTableSpec _leftSpec, _rightSpec, _joinSpec, _leftInputSpec, _rightInputSpec;
	int[] _joinleftcolmap, _joinrightcolmap, _leftcolmap, _rightcolmap;

	SpecGenerator(DataTableSpec leftInputSpec, DataTableSpec rightInputSpec) 
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

	// return a spec for the left minus right columns 
	private DataTableSpec specMinus(DataTableSpec leftSpec, DataTableSpec rightSpec) {
		DataColumnSpec[] cols = leftSpec.stream()
				.filter(s -> !rightSpec.containsName(s.getName()))
				.toArray(DataColumnSpec[]::new);
			return new DataTableSpec(cols);
	}

	// return a spec for the join columns
	private DataTableSpec getJoinSpec(DataTableSpec leftSpec, DataTableSpec rightSpec) 
	throws InvalidSettingsException {
		DataColumnSpec[] jcols = leftSpec.stream()
			.filter(s -> rightSpec.containsName(s.getName()))
			.toArray(DataColumnSpec[]::new);
		for (DataColumnSpec jcol : jcols) {
			DataColumnSpec rcol = rightSpec.getColumnSpec(jcol.getName());
			if (rcol != null && rcol.getType() != jcol.getType())
				throw new InvalidSettingsException("Join columns not same type: " + jcol.getName());
		}
		return new DataTableSpec(jcols);
	}
}

