package org.andl.ra.join;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Implement dialog for selecting which join operation.
 *  
 * @author andl
 */
public class RaJoinNodeDialog extends DefaultNodeSettingsPane {

    protected RaJoinNodeDialog() {
		SettingsModelString settings = RaJoinNodeModel.createSettingsModel();
		addDialogComponent(new DialogComponentStringSelection(settings, "Join Operation", 
				RaJoinNodeModel.ALL_OPERATIONS));
    }
}

