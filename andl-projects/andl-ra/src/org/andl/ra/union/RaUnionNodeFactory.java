package org.andl.ra.union;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * This is an example implementation of the node factory of the
 * "RaSet" node.
 *
 * @author andl
 */
public class RaUnionNodeFactory 
        extends NodeFactory<RaUnionNodeModel> {

    /** {@inheritDoc} */
    @Override
    public RaUnionNodeModel createNodeModel() {
        return new RaUnionNodeModel();
    }

    /** {@inheritDoc} */
    @Override
    public int getNrNodeViews() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public NodeView<RaUnionNodeModel> createNodeView(final int viewIndex,
            final RaUnionNodeModel nodeModel) {
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
        return new RaUnionNodeDialog();
    }

}

