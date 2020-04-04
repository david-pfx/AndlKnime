package org.andl.ra.rename;

import org.andl.ra.value.RaValueNodeDialog;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.defaultnodesettings.SettingsModelStringArray;

/**
 * <code>NodeDialog</code> for the "RaRename" node.
 * 
 * @author David Bennett -- Andl
 */
public class RaRenameNodeDialog extends DefaultNodeSettingsPane {
	
	private static final NodeLogger LOGGER = NodeLogger.getLogger(RaValueNodeDialog.class);
	private static final String TAB_TITLE = "Attribute Selection";
	
	SettingsModelString _oldColName = new SettingsModelString("old-name", "");
	SettingsModelString _newColName = new SettingsModelString("new-name", "");

    /**
     * New pane for configuring the RaRename node.
     * @throws NotConfigurableException 
     */
    protected RaRenameNodeDialog() {
    	LOGGER.debug("New");
    	setDefaultTabTitle(TAB_TITLE);
    }
    
    // deconstruct loaded string array values
    @Override
	public void loadAdditionalSettingsFrom(NodeSettingsRO settings, DataTableSpec[] specs) throws NotConfigurableException {
    	LOGGER.debug("load from");
    	if (specs == null || specs[0] == null || specs[0].getNumColumns() == 0)
            throw new NotConfigurableException("No attributes available for selection.");

    	SettingsModelStringArray oldcolsettings = RaRenameNodeModel.createSettingsOldColumnNames();
    	try {
    		oldcolsettings.loadSettingsFrom(settings);
    		String[] oldcols = oldcolsettings.getStringArrayValue();
    		_oldColName = new SettingsModelString("old", oldcols.length > 0 ? oldcols[0] : "");
    	} catch(InvalidSettingsException e) {
    		_oldColName = new SettingsModelString("old", "");
    	}

    	SettingsModelStringArray newcolsettings = RaRenameNodeModel.createSettingsNewColumnNames();
    	try {
    		newcolsettings.loadSettingsFrom(settings);
    		String[] newcols = newcolsettings.getStringArrayValue();
    		_newColName = new SettingsModelString("new", newcols.length > 0 ? newcols[0] : "");
    	} catch(InvalidSettingsException e) {
    		_newColName = new SettingsModelString("new", "");
    	}
    	
    	addSelectionTab(specs[0].getColumnNames());
    }    

    // create and add the default tab, replacing any existing
    // the parent DefaultNodeSettingsPane hosts whatever DialogComponents we then add
    // the effect is a dynamic dialog    
    private void addSelectionTab(String[] columnNames) {
    	removeTab(TAB_TITLE);
    	createNewTab(TAB_TITLE);
    	selectTab(TAB_TITLE);
    	addDialogComponent(new DialogComponentStringSelection(_oldColName, "Attribute to rename", columnNames));
		addDialogComponent(new DialogComponentString(_newColName, "New attribute name"));
	}

	// construct and save string array values
    @Override
	public void saveAdditionalSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
    	LOGGER.debug("save add to");

    	SettingsModelStringArray oldcols = RaRenameNodeModel.createSettingsOldColumnNames();
    	oldcols.setStringArrayValue(new String[] { _oldColName.getStringValue() });
		oldcols.saveSettingsTo(settings);
		
		SettingsModelStringArray newcols = RaRenameNodeModel.createSettingsNewColumnNames();
    	newcols.setStringArrayValue(new String[] { _newColName.getStringValue() });
    	newcols.saveSettingsTo(settings);
    }
    
}

