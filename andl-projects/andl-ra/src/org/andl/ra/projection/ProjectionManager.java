package org.andl.ra.projection;

import java.util.HashSet;

import org.andl.ra.RaTuple;
import org.knime.base.node.preproc.filter.row.RowFilterIterator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.RowIterator;
import org.knime.core.data.container.ColumnRearranger;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.util.filter.NameFilterConfiguration.FilterResult;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterConfiguration;

/**
 * Implement projection algorithm
 */
class ProjectionManager {

	ColumnRearranger _colre;
    
    DataTableSpec getTableSpec() {
		return _colre.createSpec();
	}

    // ctor: set up tablespec using ColumnRearranger
	ProjectionManager(DataTableSpec spec, DataColumnSpecFilterConfiguration m_conf) {
        final FilterResult filter = m_conf.applyTo(spec);
        _colre = new ColumnRearranger(spec);
        _colre.keepOnly(filter.getIncludes());
    }

	// implement projection algorithm using a row iterator and container
	BufferedDataTable execute(final BufferedDataTable inData, final ExecutionContext exec) 
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
                RaTuple tuple = new RaTuple(row);
            	if (!tupleSet.contains(tuple)) {
	                count++;
	                tupleSet.add(tuple);
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