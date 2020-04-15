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

import org.knime.core.node.NodeView;

/**
 * <code>NodeView</code> for the "RaValue" node.
 *
 * @author andl
 */
public class RaValueNodeView extends NodeView<RaValueNodeModel> {

    /**
     * Creates a new view.
     * 
     * @param nodeModel The model (class: {@link RaValueNodeModel})
     */
    protected RaValueNodeView(final RaValueNodeModel nodeModel) {
        super(nodeModel);
        // TODO: generated method stub
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void modelChanged() {
        // TODO: generated method stub
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void onClose() {
        // TODO: generated method stub
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void onOpen() {
        // TODO: generated method stub
    }

}

