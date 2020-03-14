package org.andl.ra.union;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * Implement dialog for selecting which set operation.
 *  
 * @author andl
 */
public class RaUnionNodeDialog extends DefaultNodeSettingsPane {

    protected RaUnionNodeDialog() {
        super();
        
		SettingsModelString settings = RaUnionNodeModel.createSettingsModel();
		addDialogComponent(new DialogComponentStringSelection(settings, "Set Operation", 
				RaUnionNodeModel.ALL_OPERATIONS));
    }
}

