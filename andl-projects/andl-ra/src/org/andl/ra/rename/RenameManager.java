package org.andl.ra.rename;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.andl.ra.RaTuple;
import org.knime.base.node.preproc.filter.row.RowFilterIterator;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.RowIterator;
import org.knime.core.data.container.ColumnRearranger;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.util.filter.NameFilterConfiguration.FilterResult;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterConfiguration;

/**
 * Implement projection algorithm
 * 
 * rename only has to create a new spec and replace it
 */
class RenameManager {

	DataTableSpec _outSpec;
	
    DataTableSpec getTableSpec() { return _outSpec; }

    // ctor: set up tablespec 
	RenameManager(DataTableSpec inSpec, String[] oldColumns, String[] newColumns)
	throws InvalidSettingsException {
		_outSpec = createSpec(inSpec, oldColumns, newColumns);
    }

    // create a table spec with renames
	private DataTableSpec createSpec(final DataTableSpec inSpec, String[] oldcolnames, String[] newcolnames)
	throws InvalidSettingsException {
		if (oldcolnames.length != newcolnames.length)
    		throw new InvalidSettingsException("mismatched old and new names");
    	if (oldcolnames.length == 0)
    		return inSpec;
        
        HashMap<String,String> map = new HashMap<String,String>();
        for (int i = 0; i < oldcolnames.length; ++i) {
        	String key = oldcolnames[i]; 
        	if (map.containsKey(key))
        		throw new InvalidSettingsException("Duplicate rename for column: " + key);
        	map.put(key, newcolnames[i]);
        }
        
        ArrayList<DataColumnSpec> newspecs = new ArrayList<DataColumnSpec>();
        for (DataColumnSpec spec : inSpec) {
        	if (map.containsKey(spec.getName())) {
        		DataColumnSpecCreator creator = new DataColumnSpecCreator(spec);
        		creator.setName(map.get(spec.getName()));
        		newspecs.add(creator.createSpec());
        	} else 
        		newspecs.add(spec);        		
        }
        return new DataTableSpec(newspecs.toArray(new DataColumnSpec[newspecs.size()]));
	}

	// implement rename algorithm by simply replacing the spec
	BufferedDataTable execute(final BufferedDataTable inData, final ExecutionContext exec) {
		
        return exec.createSpecReplacerTable(inData, _outSpec);
	}
}