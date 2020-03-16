package org.andl.ra.rename;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.andl.ra.extension.RaExtensionNodeModel;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelStringArray;

/**
 * <code>NodeModel</code> for the "RaRename" node.
 *
 * @author David Bennett -- Andl
 */
public class RaRenameNodeModel extends NodeModel {
    
	private static final NodeLogger LOGGER = NodeLogger.getLogger(RaExtensionNodeModel.class);
	private static final String KEY_OLD_COLUMN_NAMES = "old-column-names";
	private static final String KEY_NEW_COLUMN_NAMES = "new-column-names";

	private final SettingsModelStringArray _oldColumnNameSettings = createSettingsOldColumnNames();
	private final SettingsModelStringArray _newColumnNameSettings = createSettingsNewColumnNames();

	private static String[] _columnNames = new String[0];

    /**
     * Constructor for the node model.
     */
    protected RaRenameNodeModel() {
        super(1, 1);
        LOGGER.info("Rename node created");
    }

    // get settings model for old column names
	static SettingsModelStringArray createSettingsOldColumnNames() {
		return new SettingsModelStringArray(KEY_OLD_COLUMN_NAMES, new String[0]);
	}

	// get settings model for new column names
	static SettingsModelStringArray createSettingsNewColumnNames() {
		return new SettingsModelStringArray(KEY_NEW_COLUMN_NAMES, new String[0]);
	}
	
	// get list of old column names
	static String[] getColumNames() {
		return _columnNames;
	}

    /** {@inheritDoc} */
    @Override
    protected BufferedDataTable[] execute(final BufferedDataTable[] inData,
            final ExecutionContext exec) throws Exception {

        BufferedDataTable in = inData[0];
        BufferedDataTable out = exec.createSpecReplacerTable(in, createNewSpec(in.getDataTableSpec()));
        return new BufferedDataTable[] { out };
    }

    /** {@inheritDoc} */
    @Override
    protected void reset() { }

    /** {@inheritDoc} */
    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs)
    throws InvalidSettingsException {

    	// get column names for use by dialog
    	_columnNames = inSpecs[0].getColumnNames();
        return new DataTableSpec[] { createNewSpec(inSpecs[0]) };
    }

    /** {@inheritDoc} */
    DataTableSpec createNewSpec(final DataTableSpec inSpec) throws InvalidSettingsException {
    	// parallel arrays of old and new names, possibly empty
    	return createSpec(inSpec, 
    			_oldColumnNameSettings.getStringArrayValue(), 
    			_newColumnNameSettings.getStringArrayValue());
    }

    /** {@inheritDoc} */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
		_oldColumnNameSettings.saveSettingsTo(settings);
		_newColumnNameSettings.saveSettingsTo(settings);
    }

    /** {@inheritDoc} */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {
		_oldColumnNameSettings.loadSettingsFrom(settings);
		_newColumnNameSettings.loadSettingsFrom(settings);
    }

    /** {@inheritDoc} */
    @Override
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {
		_oldColumnNameSettings.validateSettings(settings);
		_newColumnNameSettings.validateSettings(settings);
    }
    
    /** {@inheritDoc} */
    @Override
    protected void loadInternals(final File internDir,
            final ExecutionMonitor exec) throws IOException,
            CanceledExecutionException { }
    
    /** {@inheritDoc} */
    @Override
    protected void saveInternals(final File internDir,
            final ExecutionMonitor exec) throws IOException,
            CanceledExecutionException { }

    //==========================================================================

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
        DataTableSpec outspec = new DataTableSpec(newspecs.toArray(new DataColumnSpec[newspecs.size()]));
		return outspec;
	}
    


}

