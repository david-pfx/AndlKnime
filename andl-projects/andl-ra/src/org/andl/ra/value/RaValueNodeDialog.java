package org.andl.ra.value;

import java.util.ArrayList;

import org.knime.core.data.DataType;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentMultiLineString;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.util.DefaultStringIconOption;
import org.knime.core.node.util.StringIconOption;

/**
 * <code>NodeDialog</code> for the "RaValue" node.
 * 
 * @author andl
 */
public class RaValueNodeDialog extends DefaultNodeSettingsPane {

    // New pane for configuring the RaValue node.
    protected RaValueNodeDialog() {
    	ArrayList<StringIconOption> options = new ArrayList<>();
        for (TypeCellFactory factory : TypeCellFactory.values()) {
        	DataType type = factory.getDataType();
            options.add(new DefaultStringIconOption(factory.name(), type.getIcon()));
        }
		addDialogComponent(new DialogComponentString(
				RaValueNodeModel.createSettingsColumnName(), "New attribute name"));
		addDialogComponent(new DialogComponentStringSelection(
				RaValueNodeModel.createSettingsColumnTypeName(), "New value type",
				options.toArray(new StringIconOption[options.size()])));
		addDialogComponent(new DialogComponentMultiLineString(
				RaValueNodeModel.createSettingsExpression(), "Value expression"));
    }
}



