/*******************************************************************************
 * Andl Extended Relational Algebra Nodes for Knime
 * 
 * Andl is A New Data Language. See andl.org.
 *  
 * Copyright (c) David M. Bennett 2020 as an unpublished work.
 *  
 * Rights to copy, modify and distribute this work are granted under the terms of a licence agreement.
 * See readme.md for details.
 *  
 *******************************************************************************/

package org.andl.ra.value;

import java.util.ArrayList;

import org.andl.ra.RaType;
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
        for (RaType factory : RaType.values()) {
        	DataType type = factory.getDataType();
            options.add(new DefaultStringIconOption(factory.name(), type.getIcon()));
        }
		addDialogComponent(new DialogComponentString(
				RaValueNodeModel.createSettingsColumnName(), "New attribute name"));
		addDialogComponent(new DialogComponentStringSelection(
				RaValueNodeModel.createSettingsColumnType(), "New value type",
				options.toArray(new StringIconOption[options.size()])));
		addDialogComponent(new DialogComponentMultiLineString(
				RaValueNodeModel.createSettingsNewExpression(), "Value expression"));
    }
}



