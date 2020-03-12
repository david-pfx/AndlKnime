package org.andl.ra.rename;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
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
	
	SettingsModelString _oldColName = new SettingsModelString("old-name", "");
	SettingsModelString _newColName = new SettingsModelString("new-name", "");

    /**
     * New pane for configuring the RaRename node.
     * @throws NotConfigurableException 
     */
    protected RaRenameNodeDialog() {
		addDialogComponent(new DialogComponentStringSelection(_oldColName, "Column to rename",
				RaRenameNodeModel.getColumNames()));
		addDialogComponent(new DialogComponentString(_newColName, "New column name"));
    }
    
    // deconstruct loaded string array values
    @Override
	public void loadSettingsFrom(NodeSettingsRO settings, DataTableSpec[] specs) throws NotConfigurableException {
    	if (specs == null || specs[0] == null || specs[0].getNumColumns() == 0)
            throw new NotConfigurableException("No columns available for selection.");

    	String[] oldcols = RaRenameNodeModel.createSettingsOldColumnNames().getStringArrayValue();
		_oldColName = new SettingsModelString("old", oldcols.length > 0 ? oldcols[0] : "");
		String[] newcols = RaRenameNodeModel.createSettingsNewColumnNames().getStringArrayValue();
		_newColName = new SettingsModelString("new", newcols.length > 0 ? newcols[0] : "");
    }    

    // construct and save string array values
    @Override
	public void saveAdditionalSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {

    	SettingsModelStringArray oldcols = RaRenameNodeModel.createSettingsOldColumnNames();
    	oldcols.setStringArrayValue(new String[] { _oldColName.getStringValue() });
		
		SettingsModelStringArray newcols = RaRenameNodeModel.createSettingsNewColumnNames();
    	newcols.setStringArrayValue(new String[] { _newColName.getStringValue() });
    }
    
}

