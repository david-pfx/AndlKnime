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

