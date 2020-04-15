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

package org.andl.ra.aggregation;

import java.util.Arrays;
import java.util.List;

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
	private static final String INCL_TITLE = "Available for Aggregation";

	private final SettingsModelFilterString _columnFilter = RaAggregationNodeModel.createColumnFilter();
	SettingsModelString _unaggColumn = RaAggregationNodeModel.createUnaggColumn();
	SettingsModelString _aggColumn = RaAggregationNodeModel.createAggColumn();
	SettingsModelString _aggFunction = RaAggregationNodeModel.createAggFunction();
    
    protected RaAggregationNodeDialog() {
    	LOGGER.debug("*aggregation dialog created");

    	setDefaultTabTitle(TAB_COLUMN_TITLE);
    	DialogComponentColumnFilter dcf = new DialogComponentColumnFilter(_columnFilter, 0, true);
		dcf.setIncludeTitle(INCL_TITLE);
		dcf.setExcludeTitle(EXCL_TITLE);
		addDialogComponent(dcf);

		addSelectionTab();
		_columnFilter.addChangeListener(e -> {
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
    	
    	List<String> groupCols = _columnFilter.getExcludeList();
    	List<String> availCols = _columnFilter.getIncludeList();
    	List<String> availFuncs = AggFunction.getAllNames(); //TODO: filter by type
    	addDialogComponent(new DialogComponentLabel("Grouping columns: " + groupCols));
    	addDialogComponent(new DialogComponentLabel("Available columns: " + availCols));
    	addDialogComponent(new DialogComponentLabel("Available functions: " + availFuncs));
    	
    	if (availCols.size() == 0) {
        	addDialogComponent(new DialogComponentLabel("No columns available for selection"));    		
    	} else {
    		// make sure everything has valid values going in
    		if (!availCols.contains(_unaggColumn.getStringValue()))
    			_unaggColumn.setStringValue(availCols.get(0));

	    	if (!availFuncs.contains(_aggFunction.getStringValue()))
	    		_aggFunction.setStringValue(availFuncs.get(0));
	    	
	    	if ("".equals(_aggColumn.getStringValue()))
	    		_aggColumn.setStringValue("<aggregate>");
//	    		_unaggColumn.setStringValue(_aggFunction.getStringValue() + "(" +
//	    				_unaggColumn.getStringValue() + ")");

    		addDialogComponent(new DialogComponentStringSelection(_unaggColumn, "Column to Aggregate", 
    				availCols.toArray(new String[0])));
    		addDialogComponent(new DialogComponentStringSelection(_aggFunction, "Aggregation function", 
    				availFuncs.toArray(new String[0])));
    		addDialogComponent(new DialogComponentString(_aggColumn, "Name for Aggregated Column"));
    	}
	}
}

