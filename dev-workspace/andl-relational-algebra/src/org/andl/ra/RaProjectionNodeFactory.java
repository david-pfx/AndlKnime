package org.andl.ra;

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

    /**
     * {@inheritDoc}
     */
    @Override
    public RaProjectionNodeModel createNodeModel() {
        return new RaProjectionNodeModel();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNrNodeViews() {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeView<RaProjectionNodeModel> createNodeView(final int viewIndex,
            final RaProjectionNodeModel nodeModel) {
		return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasDialog() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeDialogPane createNodeDialogPane() {
        return new RaProjectionNodeDialog();
    }

}

