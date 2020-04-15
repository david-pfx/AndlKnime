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

package org.andl.ra.selection;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentMultiLineString;

/**
 * <code>NodeDialog</code> for the "RaSelection" node.
 * 
 * @author David Bennett --- Andl
 */
public class RaSelectionNodeDialog extends DefaultNodeSettingsPane {

    // New pane for configuring the RaSelection node.
    protected RaSelectionNodeDialog() {
		addDialogComponent(new DialogComponentMultiLineString(
			RaSelectionNodeModel.createSettingsExpression(), "Selection Expression"));

    }
}

