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

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * This is an example implementation of the node factory of the
 * "RaJoin" node.
 *
 * @author andl
 */
public class RaJoinNodeFactory 
        extends NodeFactory<RaJoinNodeModel> {

    /** {@inheritDoc} */
    @Override
    public RaJoinNodeModel createNodeModel() {
        return new RaJoinNodeModel();
    }

    /** {@inheritDoc} */
    @Override
    public int getNrNodeViews() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public NodeView<RaJoinNodeModel> createNodeView(final int viewIndex,
            final RaJoinNodeModel nodeModel) {
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
        return new RaJoinNodeDialog();
    }

}

