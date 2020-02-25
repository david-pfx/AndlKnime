package org.andl.ra.projection;

import org.knime.core.node.NodeView;


/**
 * This is an example implementation of the node view of the
 * "RaProjection" node.
 * 
 * As this example node does not have a view, this is just an empty stub of the 
 * NodeView class which not providing a real view pane.
 *
 * @author andl
 */
public class RaProjectionNodeView extends NodeView<RaProjectionNodeModel> {

    /**
     * Creates a new view.
     * 
     * @param nodeModel The model (class: {@link RaProjectionNodeModel})
     */
    protected RaProjectionNodeView(final RaProjectionNodeModel nodeModel) {
        super(nodeModel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void modelChanged() {
        RaProjectionNodeModel nodeModel = 
            (RaProjectionNodeModel)getNodeModel();
        assert nodeModel != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void onClose() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void onOpen() {
    }

}

