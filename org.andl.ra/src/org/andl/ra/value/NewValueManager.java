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

package org.andl.ra.value;

import java.util.HashSet;

import org.andl.ra.RaEvaluator;
import org.andl.ra.RaTuple;
import org.andl.ra.RaType;
import org.knime.base.node.preproc.filter.row.RowFilterIterator;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.RowIterator;
import org.knime.core.data.container.CellFactory;
import org.knime.core.data.container.ColumnRearranger;
import org.knime.core.data.container.SingleCellFactory;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;

/**
 *  Implementation of new value algorithm
 *  Based on evaluating expression using Jexl.
 */
class NewValueManager {
	RaType _tcf;
	DataColumnSpec _outColSpec;
	ColumnRearranger _colre;
	RaEvaluator _jexl;
	boolean _isReplace;
	
	DataTableSpec getTableSpec() { 
		return _colre.createSpec(); 
	}
	
	// construct the rearranger, table spec, jexl, etc
	NewValueManager(DataTableSpec inspec, String colname, String typename, String expression)
	throws InvalidSettingsException {
		_tcf = RaType.valueOf(typename);
		_outColSpec = new DataColumnSpecCreator(colname, _tcf.getDataType()).createSpec();

		_jexl = new RaEvaluator(inspec, _outColSpec.getType(), expression);
		_colre = new ColumnRearranger(inspec);
		_isReplace = (_colre.indexOf(colname) >= 0);
		CellFactory factory = new SingleCellFactory(_outColSpec) {
			@Override
			public DataCell getCell(final DataRow row) {
				return _jexl.evaluateDataCell(row);
			}
		};
		if (_isReplace)
			_colre.replace(factory, colname);
		else _colre.append(factory);
	}

	// carry out the new value operation, dedup as needed
	BufferedDataTable execute(BufferedDataTable inData, ExecutionContext exec) 
	throws CanceledExecutionException {
		BufferedDataTable tempTable = exec.createColumnRearrangeTable(inData, _colre, exec);
        BufferedDataContainer container = exec.createDataContainer(tempTable.getDataTableSpec());
        
        exec.setMessage("Searching first matching row...");
        int count = 0;
        HashSet<RaTuple> tupleSet = new HashSet<RaTuple>();
        RowIterator iter = tempTable.iterator();
        try {
            while (iter.hasNext()) {
                DataRow row = iter.next();
                boolean add = true;
                if (_isReplace) { // need to dedupe
	                RaTuple tuple = new RaTuple(row);
	            	if (tupleSet.contains(tuple)) add = false;
	            	else tupleSet.add(tuple);
                } 
                if (add) {
	                count++;
	                container.addRowToTable(row);
	                exec.setMessage("Added row " + count);
                }
            }
        } catch (RowFilterIterator.RuntimeCanceledExecutionException rce) {
            throw rce.getCause();
        } finally {
            container.close();
        }
		return container.getTable();
	}
}
