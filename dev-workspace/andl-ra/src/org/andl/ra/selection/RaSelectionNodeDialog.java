package org.andl.ra.selection;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentMultiLineString;

/**
 * <code>NodeDialog</code> for the "RaSelection" node.
 * 
 * @author David Bennett --- Andl
 */
public class RaSelectionNodeDialog extends DefaultNodeSettingsPane {

    /**
     * New pane for configuring the RaSelection node.
     */
    protected RaSelectionNodeDialog() {
		addDialogComponent(new DialogComponentMultiLineString(
				RaSelectionNodeModel.createSettingsExpression(), "Boolean expression"));

    }
}

