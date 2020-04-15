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

package org.andl.ra.projection;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * Implements the node factory of the "RaProjection" node.
 *
 * @author andl
 */
public class RaProjectionNodeFactory 
        extends NodeFactory<RaProjectionNodeModel> {

    /** {@inheritDoc} */
    @Override
    public RaProjectionNodeModel createNodeModel() {
        return new RaProjectionNodeModel();
    }

    /** {@inheritDoc} */
    @Override
    public int getNrNodeViews() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public NodeView<RaProjectionNodeModel> createNodeView(final int viewIndex,
            final RaProjectionNodeModel nodeModel) {
		return null;
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasDialog() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public NodeDialogPane createNodeDialogPane() {
        return new RaProjectionNodeDialog();
    }

}

