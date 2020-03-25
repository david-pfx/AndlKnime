package org.andl.ra.aggregation;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentLabel;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.defaultnodesettings.SettingsModelStringArray;
import org.knime.core.node.util.filter.NameFilterConfiguration.FilterResult;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterConfiguration;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterPanel;

//Dialog for selecting projection columns. 
public class RaAggregationNodeDialog extends DefaultNodeSettingsPane {

	private static final NodeLogger LOGGER = NodeLogger.getLogger(RaAggregationNodeDialog.class);
	private static final String TAB_COLUMN_TITLE = "Attributes to Aggregate";
	private static final String TAB_ACTION_TITLE = "Define Aggregation";

	private final DataColumnSpecFilterPanel m_filterPanel;

    protected RaAggregationNodeDialog() {
        m_filterPanel = new DataColumnSpecFilterPanel(true);
    	this.setDefaultTabTitle(TAB_ACTION_TITLE);
        addTabAt(0, TAB_COLUMN_TITLE, m_filterPanel);
        selectTab(TAB_COLUMN_TITLE);
    }
    
    // deconstruct loaded string array values
    @Override
	public void loadAdditionalSettingsFrom(NodeSettingsRO settings, DataTableSpec[] specs) throws NotConfigurableException {
    	LOGGER.debug("load from");
    	if (specs == null || specs[0] == null || specs[0].getNumColumns() == 0)
            throw new NotConfigurableException("No attributes available for selection.");

        final DataTableSpec spec = specs[0];
        if (spec == null || spec.getNumColumns() == 0)
            throw new NotConfigurableException("No attributes available for selection.");

        // create a filter config and load it into the filter panel
        DataColumnSpecFilterConfiguration config = RaAggregationNodeModel.createDCSFilterConfiguration();
        config.loadConfigurationInDialog(settings, spec);
        m_filterPanel.loadConfiguration(config, spec);

        final FilterResult filter = config.applyTo(spec);
        final String[] incls = filter.getIncludes();
        
    	String[] names = RaAggregationNodeModel.createSettingsNewColumnNames().getStringArrayValue();
    	String[] exprs = RaAggregationNodeModel.createSettingsExpressions().getStringArrayValue();
        SettingsModelString[] namesettings = new SettingsModelString[incls.length]; 
        SettingsModelString[] exprsettings = new SettingsModelString[incls.length];
        for (int i = 0; i < incls.length; ++i) {
        	namesettings[i] = new SettingsModelString("dummy-name=" + i, 
        			i < names.length ? names[i] : "sum(" + incls[i] + ")");
        	exprsettings[i] = new SettingsModelString("dummy-expr=" + i, i < exprs.length ? exprs[i] : "sum");
        }
    	addSelectionTab(incls, namesettings, exprsettings);
    }    

	// construct and save string array values
    @Override
	public void saveAdditionalSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
    	LOGGER.debug("save add to");

    	DataColumnSpecFilterConfiguration config = RaAggregationNodeModel.createDCSFilterConfiguration();
        m_filterPanel.saveConfiguration(config);
        config.saveConfiguration(settings);
    }
    
    // create and add the default tab, replacing any existing
    // the parent DefaultNodeSettingsPane hosts whatever DialogComponents we then add
    // the effect is a dynamic dialog    
    private void addSelectionTab(String[] columnNames, SettingsModelString[] typesettings, SettingsModelString[] exprsettings) {
    	removeTab(TAB_ACTION_TITLE);
    	createNewTab(TAB_ACTION_TITLE);
    	selectTab(TAB_ACTION_TITLE);
    	addDialogComponent(new DialogComponentLabel(String.format("cols=%d", columnNames.length)) );
    	
    	for (int i = 0; i < columnNames.length; ++i) {
        	addDialogComponent(new DialogComponentString(typesettings[i], "New column name"));
        	setHorizontalPlacement(true);
        	addDialogComponent(new DialogComponentString(exprsettings[i], "Aggregation function"));
        	setHorizontalPlacement(false);    		
    	}
    	//addDialogComponent(new DialogComponentStringSelection(_oldColName, "Attribute to rename", columnNames));
		//addDialogComponent(new DialogComponentString(_newColName, "New attribute name"));
	}

    
}

