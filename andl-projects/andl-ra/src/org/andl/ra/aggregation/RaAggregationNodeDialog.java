package org.andl.ra.aggregation;

import java.util.Arrays;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnFilter;
import org.knime.core.node.defaultnodesettings.DialogComponentLabel;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelFilterString;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

//Dialog for selecting projection columns. 
public class RaAggregationNodeDialog extends DefaultNodeSettingsPane {

	private static final NodeLogger LOGGER = NodeLogger.getLogger(RaAggregationNodeDialog.class);
	private static final String TAB_COLUMN_TITLE = "Select Grouping";
	private static final String TAB_ACTION_TITLE = "Define Aggregation";
	private static final String EXCL_TITLE = "Grouping Columns";
	private static final String INCL_TITLE = "Available Columns";

	private final SettingsModelFilterString _columnFilterSettings = RaAggregationNodeModel.createColumnFilterSettings();
	SettingsModelString _oldColumnNameSettings = RaAggregationNodeModel.createSettingsOldColumnName();
	SettingsModelString _newColumnNameSettings = RaAggregationNodeModel.createSettingsNewColumnName();
	SettingsModelString _aggFunctionSettings = RaAggregationNodeModel.createSettingsAggFunction();
    
    protected RaAggregationNodeDialog() {
    	LOGGER.debug("*aggregation dialog created");

    	setDefaultTabTitle(TAB_COLUMN_TITLE);
    	DialogComponentColumnFilter dcf = new DialogComponentColumnFilter(_columnFilterSettings, 0, true);
		dcf.setIncludeTitle(INCL_TITLE);
		dcf.setExcludeTitle(EXCL_TITLE);
		addDialogComponent(dcf);

		addSelectionTab();
		_columnFilterSettings.addChangeListener(e -> {
	    	addSelectionTab();
		});
        selectTab(TAB_COLUMN_TITLE);
    }
    
    // just a sanity check
    @Override
	public void loadAdditionalSettingsFrom(NodeSettingsRO settings, DataTableSpec[] specs) throws NotConfigurableException {
    	DataTableSpec spec = specs[0];
    	LOGGER.debug("*load add from " + settings + " spec=" + spec);
    	if (spec.getNumColumns() == 0)
            throw new NotConfigurableException("No columns available for selection.");
    }    

	// just helps with debugging
    @Override
	public void saveAdditionalSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
    	LOGGER.debug("*save add to " + settings);

    }

    // add a new tab dynamically to select aggregated column, function and new name
    private void addSelectionTab() {
    	removeTab(TAB_ACTION_TITLE);
    	createNewTab(TAB_ACTION_TITLE);
    	
    	String[] availCols = _columnFilterSettings.getIncludeList().toArray(new String[0]);
    	String[] availFuncs = AggFunction.getAllNames().toArray(new String[0]); //TODO: filter by type
    	addDialogComponent(new DialogComponentLabel("Grouping columns: " + 
    			_columnFilterSettings.getExcludeList()));
    	addDialogComponent(new DialogComponentLabel(String.format("Available columns: %s", 
    			_columnFilterSettings.getIncludeList())));
    	addDialogComponent(new DialogComponentLabel(String.format("Available functions: %s", 
    			Arrays.asList(availFuncs))));
    	
    	if (availCols.length == 0) {
        	addDialogComponent(new DialogComponentLabel("No columns available for selection"));    		
    	} else {
    		// make sure everything has valid values going in
    		if (!Arrays.asList(availCols).contains(_oldColumnNameSettings.getStringValue()))
    			_oldColumnNameSettings.setStringValue(availCols[0]);

	    	if (!Arrays.asList(availFuncs).contains(_aggFunctionSettings.getStringValue()))
	    		_aggFunctionSettings.setStringValue(availFuncs[0]);
	    	
	    	if (_oldColumnNameSettings.getStringValue() == null)
	    		_oldColumnNameSettings.setStringValue(_aggFunctionSettings.getStringValue() + "(" +
	    				_oldColumnNameSettings.getStringValue() + ")");

    		addDialogComponent(new DialogComponentStringSelection(_oldColumnNameSettings, "Aggregated Column", availCols));
    		addDialogComponent(new DialogComponentStringSelection(_aggFunctionSettings, "Aggregation function", availFuncs));
    		addDialogComponent(new DialogComponentString(_newColumnNameSettings, "New column name"));
    	}
	}
}

