package org.andl.ra.extension;

import java.util.ArrayList;

import org.knime.core.data.DataType;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentMultiLineString;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.util.DefaultStringIconOption;
import org.knime.core.node.util.StringIconOption;

/**
 * <code>NodeDialog</code> for the "RaExtension" node.
 * 
 * @author andl
 */
public class RaExtensionNodeDialog extends DefaultNodeSettingsPane {

    /**
     * New pane for configuring the RaExtension node.
     */
    protected RaExtensionNodeDialog() {
    	ArrayList<StringIconOption> options = new ArrayList<>();
        for (TypeCellFactory factory : TypeCellFactory.values()) {
        	DataType type = factory.getDataType();
            options.add(new DefaultStringIconOption(factory.name(), type.getIcon()));
        }
		addDialogComponent(new DialogComponentString(
				RaExtensionNodeModel.createSettingsColumnName(), "New column name"));
		addDialogComponent(new DialogComponentStringSelection(
				RaExtensionNodeModel.createSettingsColumnTypeName(), "Column type",
				options.toArray(new StringIconOption[options.size()])));
		addDialogComponent(new DialogComponentMultiLineString(
				RaExtensionNodeModel.createSettingsExpression(), "Expression"));
    }
}



