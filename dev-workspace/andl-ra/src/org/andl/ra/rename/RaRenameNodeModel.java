package org.andl.ra.rename;

import java.util.ArrayList;
import java.util.HashMap;

import org.andl.ra.extension.RaExtensionNodeModel;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.ColumnRearranger;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelStringArray;
import org.knime.core.node.streamable.simple.SimpleStreamableFunctionNodeModel;

/**
 * <code>NodeModel</code> for the "RaRename" node.
 *
 * @author David Bennett -- Andl
 */
public class RaRenameNodeModel extends SimpleStreamableFunctionNodeModel {
    
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
        super();
        LOGGER.info("Rename node created");
    }

	static SettingsModelStringArray createSettingsOldColumnNames() {
		return new SettingsModelStringArray(KEY_OLD_COLUMN_NAMES, new String[0]);
	}

	static SettingsModelStringArray createSettingsNewColumnNames() {
		return new SettingsModelStringArray(KEY_NEW_COLUMN_NAMES, new String[0]);
	}
	
	static String[] getColumNames() {
		return _columnNames;
	}

    /**
     * {@inheritDoc}
     */
//    @Override
//    protected BufferedDataTable[] execute(final BufferedDataTable[] inData,
//            final ExecutionContext exec) throws Exception {
//
//        BufferedDataTable in = inData[0];
//        ColumnRearranger r = createColumnRearranger(in.getDataTableSpec());
//        BufferedDataTable out = exec.createColumnRearrangeTable(in, r, exec);
//        return new BufferedDataTable[] { out };
//    }
//
//    /** {@inheritDoc} */
//    @Override
//    protected void reset() { }

    /** {@inheritDoc} */
    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs)
    throws InvalidSettingsException {

    	// get column names for use by dialog
    	_columnNames = inSpecs[0].getColumnNames();
        return super.configure(inSpecs);
    }

    /** {@inheritDoc} */
    @Override
    protected ColumnRearranger createColumnRearranger(final DataTableSpec inSpec) throws InvalidSettingsException {
    	// parallel arrays of old and new names, possibly empty
        String[] oldcolnames = _oldColumnNameSettings.getStringArrayValue();
        String[] newcolnames = _newColumnNameSettings.getStringArrayValue();
    	if (oldcolnames.length != newcolnames.length)
    		throw new InvalidSettingsException("mismatched old and new names");
    	if (oldcolnames.length == 0)
    		return new ColumnRearranger(inSpec);		// nothing to do
        
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
        ColumnRearranger rearranger = new ColumnRearranger(outspec);
        return rearranger;
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
//    @Override
//    protected void loadInternals(final File internDir,
//            final ExecutionMonitor exec) throws IOException,
//            CanceledExecutionException { }
//    
//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    protected void saveInternals(final File internDir,
//            final ExecutionMonitor exec) throws IOException,
//            CanceledExecutionException { }


}

