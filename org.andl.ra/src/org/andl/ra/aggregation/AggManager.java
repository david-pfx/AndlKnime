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

/**
 * Aggregation Manager
 */

package org.andl.ra.aggregation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.andl.ra.RaTuple;
import org.andl.ra.RaType;
import org.knime.base.node.preproc.filter.row.RowFilterIterator;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.RowIterator;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;

/*******************************************************************************
 * Aggregation function as enumeration plus extras
 */
enum AggFunction {
	//NUL(null),
	COUNT("Count"),
	SUM("Sum"),
	AVG("Average"),
	MAX("Max"),
	MIN("Min");
	
	private String _name;
	
	String getName() { return _name; }

	// search for function by name
	static AggFunction search(String name) {
		for (AggFunction value : AggFunction.values()) {
			if (value.getName().equals(name)) return value;
		}
		return null;
	}

	// get all names as a list
	static List<String> getAllNames() {
		return Stream.of(AggFunction.values())
				.map(v -> v.getName())
				.collect(Collectors.toList());
	}
	
	// get all names as a list
	static List<String> getAllValidNames(RaType argtype) {
		return Stream.of(AggFunction.values())
				.filter(v -> v.getReturnType(argtype) != null)
				.map(v -> v.getName())
				.collect(Collectors.toList());
	}
	
	// compute function return type
	RaType getReturnType(RaType argtype) {
		return this.equals(COUNT) ? RaType.INT :
			this.equals(MAX) || this.equals(MIN) ? argtype :
			this.equals(AVG) && argtype.equals(RaType.INT) ? RaType.DOUBLE :
			argtype.equals(RaType.INT) || argtype.equals(RaType.DOUBLE) ? argtype :
			null;
	}

	AggFunction(String name) {
		_name = name;
	}
}

/*******************************************************************************
 *  
 * Functions and data to manage aggregation
 * 
 */
class AggManager {
	DataTableSpec _inputSpec, _outputSpec;
	int[] _groupColNos;
	int _unaggColNo;
	AggFunction _function = null;

	private static final NodeLogger LOGGER = NodeLogger.getLogger(AggManager.class);
	DataTableSpec getOutputSpec() { return _outputSpec; }

	// ctor set up the specs
	AggManager(DataTableSpec inputSpec, String[] groupCols, String unaggCol, String aggCol, String function) 
	throws InvalidSettingsException {
        LOGGER.debug(String.format("*ctor spec=%s gcols=%s acol=%s rcol=%s afunc=%s",
        		inputSpec, Arrays.asList(groupCols), unaggCol, aggCol, function));
		_inputSpec = inputSpec;
		_groupColNos = inputSpec.columnsToIndices(groupCols);
		_unaggColNo = inputSpec.findColumnIndex(unaggCol);
		LOGGER.assertLog(_unaggColNo >= 0, "Aggregation column not valid");
		
		_function = AggFunction.search(function);
		LOGGER.assertLog(_function != null, "Unknown function");
		
		// these checks depend on knowing the unaggregated column type
		RaType unaggtype = RaType.getRaType(_inputSpec.getColumnSpec(_unaggColNo).getType());
		if (unaggtype == null)
			throw new InvalidSettingsException("Unsupported column type: " + function);
		//LOGGER.assertLog(unaggtype != null, "Unsupported column type: " + function);
		
		RaType aggtype = _function.getReturnType(unaggtype);
		if (aggtype == null)
			throw new InvalidSettingsException("Invalid aggregation for this column: " + function);
//		LOGGER.assertLog(aggtype != null, "Unsupported agregation type: " + function);
		
		ArrayList<DataColumnSpec> outcols = new ArrayList<>();
		for (int colx : _groupColNos)
			outcols.add(_inputSpec.getColumnSpec(colx));
		outcols.add(new DataColumnSpecCreator(aggCol, aggtype.getDataType()).createSpec());
		_outputSpec = new DataTableSpec(outcols.toArray(new DataColumnSpec[0]));
		
	}

	// execute the aggregation algorithm
	BufferedDataTable execute(BufferedDataTable inData, ExecutionContext exec) 
	throws CanceledExecutionException {

		BufferedDataContainer container = exec.createDataContainer(_outputSpec);
		HashMap<RaTuple, Accumulator> tupleHash = new HashMap<>();
		RowIterator iter = inData.iterator();
		exec.setMessage("Searching first matching row...");
		try {
			int incount = 0;
			while (iter.hasNext()) {
				DataRow row = iter.next();
				RaTuple tuple = new RaTuple(row, _groupColNos);
				incount++;
				exec.setMessage("Reading row " + incount);
				DataCell cell = row.getCell(_unaggColNo);
				if (tupleHash.containsKey(tuple)) {
					tupleHash.get(tuple).accumulate(cell);
				} else {
					Accumulator accum = new Accumulator(_function, cell);
					tupleHash.put(tuple, accum);
				}
			}
			int outcount = 0;
			for (Entry<RaTuple, Accumulator> entry : tupleHash.entrySet()) {
				outcount++;
				exec.setMessage("Writing row " + outcount);
				ArrayList<DataCell> cells = new ArrayList<>(Arrays.asList(entry.getKey().getCells()));
				cells.add(entry.getValue().getResult());
				container.addRowToTable(new DefaultRow("Row" + outcount, cells));
			}
		} catch (RowFilterIterator.RuntimeCanceledExecutionException rce) {
			throw rce.getCause();
		} finally {
			container.close();
		}
		return container.getTable();
	}

}

/*******************************************************************************
 *
 * Manage computations on a single accumulator
 * 
 */

class Accumulator {
	AggFunction _function;
	RaType _accumtype;
	RaType _rettype;
	Object _accumulator;
	int _count = 1;
	
	// ctor work out what types we need
	public Accumulator(AggFunction function, DataCell initValue) {
		_function = function;
		_accumtype = RaType.getRaType(initValue.getType());
		_rettype = _function.getReturnType(_accumtype);
		_accumulator = toObject(initValue, _accumtype);
	}
	
	// update accumulator with new value (and keep count)
	void accumulate(DataCell cell) {
		_count++;
		_accumulator = accumulate(_accumulator, toObject(cell, _accumtype));
	}
	
	// get the final result
	DataCell getResult() {
		Object result = finalise();
		return toDataCell(result, _rettype);
	}

	// convert data cell to accumulator
	private Object toObject(DataCell dataCell, RaType type) {
		switch(type) {
		case BOOL: return ((BooleanCell)dataCell).getBooleanValue();
		case INT: return ((IntCell)dataCell).getIntValue();
		case STRING: return ((StringCell)dataCell).getStringValue();
		case DOUBLE: return ((DoubleCell)dataCell).getDoubleValue();
		//case DATE: return ((DateAndTimeCell)dataCell).getDateValue();
		default: return null;
		}
	}

	// convert accumulator to data cell
	private DataCell toDataCell(Object accumulator, RaType type) {
		switch (type) {
		case BOOL: return (boolean)accumulator ? BooleanCell.TRUE :BooleanCell.FALSE; 
		case INT: return new IntCell((Integer)accumulator);
		case DOUBLE: return new DoubleCell((double)accumulator);
		case STRING: return new StringCell((String)accumulator);
		//case DATE: TODO
		default: return null;
		}
	}

	// compute the final result
	private Object finalise() {
		switch (_function) {
		case COUNT: return _count;
		case AVG: 
			if (_accumtype == RaType.INT)
				return (double)(int)_accumulator / _count;
			else return (double)_accumulator / _count; 
		case SUM: 
		case MAX: 
		case MIN: return _accumulator; 
		default: break;
		}
		return null;
	}

	// compute new value for accumulator
	private Object accumulate(Object accum, Object value) {
		switch (_function) {
		case SUM: 
		case AVG: 
			switch (_accumtype) {
			case INT: return (Integer)accum + (Integer)value;
			case DOUBLE: return (double)accum + (double)value;
			default: break;
			}
		case MAX: 
			switch (_accumtype) {
			case INT: return (Integer)accum > (Integer)value ? accum : value; 
			case DOUBLE: return (double)accum > (double)value ? accum : value; 
			case STRING: return ((String)accum).compareTo((String)value) > 0 ? accum : value;
			//case DATE: TODO
			default: break;
			}
		case MIN: 
			switch (_accumtype) {
			case INT: return (Integer)accum < (Integer)value ? accum : value; 
			case DOUBLE: return (double)accum < (double)value ? accum : value; 
			case STRING: return ((String)accum).compareTo((String)value) < 0 ? accum : value;
			//case DATE: TODO
			default: break;
			}
		default: break;
		}
		return null;
	}
}

