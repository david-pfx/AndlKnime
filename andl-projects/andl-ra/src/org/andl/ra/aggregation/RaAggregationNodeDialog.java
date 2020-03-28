package org.andl.ra.aggregation;

import java.util.List;
import java.util.function.Function;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnFilter;
import org.knime.core.node.defaultnodesettings.DialogComponentLabel;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.SettingsModelFilterString;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

import com.sun.xml.internal.ws.util.StringUtils;

//Dialog for selecting projection columns. 
public class RaAggregationNodeDialog extends DefaultNodeSettingsPane {

	private static final NodeLogger LOGGER = NodeLogger.getLogger(RaAggregationNodeDialog.class);
	private static final String TAB_COLUMN_TITLE = "Attributes to Aggregate";
	private static final String TAB_ACTION_TITLE = "Define Aggregation";
	private static final String INCL_TITLE = "Aggregate Columns";
	private static final String EXCL_TITLE = "Retain Columns";

    //DataColumnSpecFilterConfiguration config = RaAggregationNodeModel.createDCSFilterConfiguration();
	private final SettingsModelFilterString _columnFilterSettings = RaAggregationNodeModel.createSettingsColumnFilter();
	SettingsModelString[] _newColumnNameSettings = RaAggregationNodeModel.createSettingsNewColumnNames(0);
	SettingsModelString[] _newExpressionsSettings = RaAggregationNodeModel.createSettingsExpressions(0);
    
	String[] _columnNames = new String[0];      
	//private final DataColumnSpecFilterPanel _filterPanel;

    protected RaAggregationNodeDialog() {
    	LOGGER.debug("aggregation dialog created");

    	setDefaultTabTitle(TAB_COLUMN_TITLE);
    	DialogComponentColumnFilter dcf = new DialogComponentColumnFilter(_columnFilterSettings, 0, true);
		dcf.setIncludeTitle(INCL_TITLE);
		dcf.setExcludeTitle(EXCL_TITLE);
		addDialogComponent(dcf);

		createNewTab(TAB_ACTION_TITLE);
		_columnFilterSettings.addChangeListener(e -> {
	        final List<String> incls = _columnFilterSettings.getIncludeList();
	    	addSelectionTab(incls);
		});
        selectTab(TAB_COLUMN_TITLE);
    	
    	//_filterPanel = new DataColumnSpecFilterPanel(true);
    	//this.setDefaultTabTitle(TAB_ACTION_TITLE);
        //addTabAt(0, TAB_COLUMN_TITLE, _filterPanel);
    }
    
    // deconstruct loaded string array values
    /**
     *
     */
    @Override
	public void loadAdditionalSettingsFrom(NodeSettingsRO settings, DataTableSpec[] specs) throws NotConfigurableException {
    	DataTableSpec spec = specs[0];
    	LOGGER.debug("load add from " + settings + "spec=" + spec);
    	if (spec.getNumColumns() == 0)
            throw new NotConfigurableException("No attributes available for selection.");

    	// keep all the original column names, and make settings for all of them
        _columnNames = spec.getColumnNames();
        
    	_newColumnNameSettings = RaAggregationNodeModel.createSettingsNewColumnNames(_columnNames.length);
    	_newExpressionsSettings = RaAggregationNodeModel.createSettingsExpressions(_columnNames.length);
        for (int i = 0; i < _columnNames.length; ++i) {
        	try {
				_newColumnNameSettings[i].loadSettingsFrom(settings);
			} catch (InvalidSettingsException e) { }
        	try {
				_newExpressionsSettings[i].loadSettingsFrom(settings);
			} catch (InvalidSettingsException e) { }
        }
        final List<String> incls = _columnFilterSettings.getIncludeList();
        updateColumnInfo(incls);
    	addSelectionTab(incls);
        selectTab(incls.size() == 0 ? TAB_COLUMN_TITLE : TAB_ACTION_TITLE);
    }    

	// construct and save string array values
    @Override
	public void saveAdditionalSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
    	LOGGER.debug("save add to " + settings);

    	// dialog components do it all
    }
    
    void updateColumnInfo(List<String> incls) {
    	Function<String, Boolean> safeIsEmpty = (String s) -> s == null || s.isEmpty();
    	for (int i = 0; i < _columnNames.length; ++i) {
    		if (incls.contains(_columnNames[i])) {
    			if (safeIsEmpty.apply(_newColumnNameSettings[i].getStringValue()))
    				_newColumnNameSettings[i].setStringValue("sum(" + _columnNames[i] + ")");
    			if (safeIsEmpty.apply(_newExpressionsSettings[i].getStringValue()))
    				_newExpressionsSettings[i].setStringValue("sum()");
    		} else {
    			_newColumnNameSettings[i].setStringValue(null);
    			_newExpressionsSettings[i].setStringValue(null);    			
    		}
    	}
    }
    
    private void addSelectionTab(List<String> incls) {
    	removeTab(TAB_ACTION_TITLE);
    	createNewTab(TAB_ACTION_TITLE);
    	addDialogComponent(new DialogComponentLabel(String.format("incls = %s", incls)) );
    	
    	for (int i = 0; i < _columnNames.length; ++i) {
    		if (incls.contains(_columnNames[i])) {
    			createNewGroup(String.format("Aggregating %s", _columnNames[i]));
	    		setHorizontalPlacement(true);
	        	addDialogComponent(new DialogComponentString(_newColumnNameSettings[i], "New column name"));
	        	addDialogComponent(new DialogComponentString(_newExpressionsSettings[i], "Aggregation function"));
	        	setHorizontalPlacement(false);    	
    		}
    	}
	}

    
}

