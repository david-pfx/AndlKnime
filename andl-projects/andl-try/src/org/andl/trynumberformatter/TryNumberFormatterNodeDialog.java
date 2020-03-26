package org.andl.trynumberformatter;

import org.knime.core.node.NodeLogger;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnFilter;
import org.knime.core.node.defaultnodesettings.DialogComponentLabel;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.DialogComponentStringListSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelFilterString;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.defaultnodesettings.SettingsModelStringArray;

/**
 * This is an example implementation of the node dialog of the
 * "TryNumberFormatter" node.
 *
 * @author andl
 */
public class TryNumberFormatterNodeDialog extends DefaultNodeSettingsPane {
	
	private static final NodeLogger LOGGER = NodeLogger.getLogger(TryNumberFormatterNodeDialog.class);
	private static final String TAB_TITLE = "Dynamic options";
	static int counter = 0;
	
    protected TryNumberFormatterNodeDialog() {
        super();
        LOGGER.debug("TryNumber debug");
        LOGGER.error("TryNumber error");
        LOGGER.warn("TryNumber warn");
        LOGGER.info("TryNumber info");
        LOGGER.coding("TryNumber coding");
        LOGGER.fatal("TryNumber fatal");
		String[] strings = new String[] { "one","two","three" };
		setDefaultTabTitle(TAB_TITLE);
        addDefaultTab(strings);
        
//		SettingsModelString stringSettings = TryNumberFormatterNodeModel.createNumberFormatSettingsModel();
//		createNewGroup("new group");
//		DialogComponent dcs = new DialogComponentString(stringSettings, "Number Format - modified x2", true, 10);
//		dcs.setToolTipText("tool tip text");
//		addDialogComponent(dcs);
//		//addDialogComponent(new DialogComponentString(stringSettings, "Number Format - modified x2", true, 10));
//		
//		SettingsModelFilterString sms = new SettingsModelFilterString("cols");
//		DialogComponent dcf = new DialogComponentColumnFilter(sms, 0, true);
//		addDialogComponent(dcf);
//		
//		String[] strings = new String[] { "one","two","three" };
//		SettingsModelStringArray smsa = new SettingsModelStringArray("strings", new String[0]);
//		DialogComponentStringListSelection dcsl = new DialogComponentStringListSelection(smsa, "String array", strings);
//		dcsl.setSizeComponents(100, 100);
//		dcsl.setVisibleRowCount(5);
//		dcsl.setToolTipText("tool tip");
//		addDialogComponent(dcsl);
    }
    
    private void addDefaultTab(String[] strings) {
    	removeTab(TAB_TITLE);
    	createNewTab(TAB_TITLE);
    	selectTab(TAB_TITLE);
    	
		SettingsModelString stringSettings = TryNumberFormatterNodeModel.createNumberFormatSettingsModel();
		createNewGroup("new group");
		DialogComponent dcs = new DialogComponentString(stringSettings, "Number Format - modified x2", true, 10);
		dcs.setToolTipText("tool tip text");
		addDialogComponent(dcs);
		
		SettingsModelFilterString sms = new SettingsModelFilterString("cols");
		DialogComponent dcf = new DialogComponentColumnFilter(sms, 0, true);
		addDialogComponent(dcf);
		
		SettingsModelStringArray smsa = new SettingsModelStringArray("strings", new String[0]);
		DialogComponentStringListSelection dcsl = new DialogComponentStringListSelection(smsa, "String array", strings);
		dcsl.setSizeComponents(100, 100);
		dcsl.setVisibleRowCount(5);
		dcsl.setToolTipText("tool tip");
		addDialogComponent(dcsl);
		
		DialogComponentLabel dclb = new DialogComponentLabel("blank");
		addDialogComponent(dclb);
		
		smsa.addChangeListener(e -> {
			dclb.setText(String.format("selected=%d counter=%d", smsa.getStringArrayValue().length, ++counter));
		});
	}

}
