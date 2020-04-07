package org.andl.ra.rename;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.andl.ra.value.RaValueNodeModel;
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
    
	private static final NodeLogger LOGGER = NodeLogger.getLogger(RaValueNodeModel.class);
	private static final String KEY_OLD_COLUMN_NAMES = "old-column-names";
	private static final String KEY_NEW_COLUMN_NAMES = "new-column-names";

	// paired arrays for those columns that will be renamed, possibly empty
	private final SettingsModelStringArray _oldColumnNames = createSettingsOldColumnNames();
	private final SettingsModelStringArray _newColumnNames = createSettingsNewColumnNames();
	private RenameManager _renameManager;

    // get settings model for old column names
	static SettingsModelStringArray createSettingsOldColumnNames() {
		return new SettingsModelStringArray(KEY_OLD_COLUMN_NAMES, new String[0]);
	}

	// get settings model for new column names
	static SettingsModelStringArray createSettingsNewColumnNames() {
		return new SettingsModelStringArray(KEY_NEW_COLUMN_NAMES, new String[0]);
	}
	
    //--------------------------------------------------------------------------
    // ctor and dummy overrides
    protected RaRenameNodeModel() {
        super(1, 1);
        LOGGER.info("Rename node created");
    }

    /** {@inheritDoc} */
    @Override
    protected void reset() { }
    
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

    //--------------------------------------------------------------------------
    // execute, configure, settings
    //
    /** {@inheritDoc} */
    @Override
    protected BufferedDataTable[] execute(final BufferedDataTable[] inData,
            final ExecutionContext exec) throws Exception {

    	return new BufferedDataTable[] {
        	_renameManager.execute(inData[0], exec) 
        };
    }

    /** {@inheritDoc} */
    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs)
    throws InvalidSettingsException {

    	_renameManager = new RenameManager(inSpecs[0], 
    		_oldColumnNames.getStringArrayValue(), 
    		_newColumnNames.getStringArrayValue());

        return new DataTableSpec[] { 
        	_renameManager.getTableSpec() 
        };
    }

    /** {@inheritDoc} */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
		_oldColumnNames.saveSettingsTo(settings);
		_newColumnNames.saveSettingsTo(settings);
    }

    /** {@inheritDoc} */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {
		_oldColumnNames.loadSettingsFrom(settings);
		_newColumnNames.loadSettingsFrom(settings);
    }

    /** {@inheritDoc} */
    @Override
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {
		_oldColumnNames.validateSettings(settings);
		_newColumnNames.validateSettings(settings);
    }
}

