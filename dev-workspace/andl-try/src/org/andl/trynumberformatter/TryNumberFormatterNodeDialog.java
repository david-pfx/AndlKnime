package org.andl.trynumberformatter;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnFilter;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.SettingsModelFilterString;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * This is an example implementation of the node dialog of the
 * "TryNumberFormatter" node.
 *
 * @author andl
 */
public class TryNumberFormatterNodeDialog extends DefaultNodeSettingsPane {
    protected TryNumberFormatterNodeDialog() {
        super();
        setDefaultTabTitle("tab title");
        
		SettingsModelString stringSettings = TryNumberFormatterNodeModel.createNumberFormatSettingsModel();
		createNewGroup("new group");
		DialogComponent dcs = new DialogComponentString(stringSettings, "Number Format - modified x2", true, 10);
		dcs.setToolTipText("tool tip text");
		addDialogComponent(dcs);
		//addDialogComponent(new DialogComponentString(stringSettings, "Number Format - modified x2", true, 10));
		
		SettingsModelFilterString sms = new SettingsModelFilterString("cols");
		DialogComponent dcf = new DialogComponentColumnFilter(sms, 0, true);
		addDialogComponent(dcf);
    }
}

