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

