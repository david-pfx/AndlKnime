package org.andl.ra.set;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * This is an example implementation of the node factory of the
 * "RaSet" node.
 *
 * @author andl
 */
public class RaSetNodeFactory 
        extends NodeFactory<RaSetNodeModel> {

    /** {@inheritDoc} */
    @Override
    public RaSetNodeModel createNodeModel() {
        return new RaSetNodeModel();
    }

    /** {@inheritDoc} */
    @Override
    public int getNrNodeViews() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public NodeView<RaSetNodeModel> createNodeView(final int viewIndex,
            final RaSetNodeModel nodeModel) {
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
        return new RaSetNodeDialog();
    }

}

