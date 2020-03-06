package org.andl.ra.set;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Implement dialog for selecting which set operation.
 *  
 * @author andl
 */
public class RaSetNodeDialog extends DefaultNodeSettingsPane {

    protected RaSetNodeDialog() {
        super();
        
		SettingsModelString settings = RaSetNodeModel.createSettingsModel();
		addDialogComponent(new DialogComponentStringSelection(settings, "Set Operation", 
				RaSetNodeModel.ALL_SET_OPERATIONS));
    }
}

