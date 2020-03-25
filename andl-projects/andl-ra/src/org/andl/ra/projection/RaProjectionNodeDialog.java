package org.andl.ra.projection;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterConfiguration;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterPanel;

// dialog for selecting projection columns. 
public class RaProjectionNodeDialog extends NodeDialogPane {

    private final DataColumnSpecFilterPanel m_filterPanel;

    public RaProjectionNodeDialog() {
        m_filterPanel = new DataColumnSpecFilterPanel(true);
        super.addTab("Projection Attributes", m_filterPanel);
    }

    /** {@inheritDoc} */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings,
            final DataTableSpec[] specs) throws NotConfigurableException {
        final DataTableSpec spec = specs[0];
        if (spec == null || spec.getNumColumns() == 0)
            throw new NotConfigurableException("No attributes available for selection.");

        // create a configuration and load it into the filter panel
        DataColumnSpecFilterConfiguration config = RaProjectionNodeModel.createDCSFilterConfiguration();
        config.loadConfigurationInDialog(settings, specs[0]);
        m_filterPanel.loadConfiguration(config, specs[0]);
    }

    /** {@inheritDoc} */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings)
            throws InvalidSettingsException {

        // create a configuration and save the filter panel into it
    	DataColumnSpecFilterConfiguration config = RaProjectionNodeModel.createDCSFilterConfiguration();
        m_filterPanel.saveConfiguration(config);
        config.saveConfiguration(settings);
    }
}

