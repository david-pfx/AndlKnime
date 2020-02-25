package org.andl.ra.projection;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterConfiguration;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterPanel;

/**
 * Implement dialog for selecting projection columns. 
 */
public class RaProjectionNodeDialog extends NodeDialogPane {

    private final DataColumnSpecFilterPanel m_filterPanel;

    /**
     * Creates a new {@link NodeDialogPane} for the column filter.
     */
    public RaProjectionNodeDialog() {
        m_filterPanel = new DataColumnSpecFilterPanel();
        super.addTab("Column Selection", m_filterPanel);
    }

    /**
     * Calls the update method of the underlying filter panel.
     * @param settings - the node settings to read from
     * @param specs - the input specifications
     * @throws NotConfigurableException if no columns are available for filtering
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings,
            final DataTableSpec[] specs) throws NotConfigurableException {
        final DataTableSpec spec = specs[0];
        if (spec == null || spec.getNumColumns() == 0) {
            throw new NotConfigurableException("No columns available for selection.");
        }

        DataColumnSpecFilterConfiguration config = RaProjectionNodeModel.createDCSFilterConfiguration();
        config.loadConfigurationInDialog(settings, specs[0]);
        m_filterPanel.loadConfiguration(config, specs[0]);
    }

    /**
     * Sets the list of columns to include inside the corresponding
     * <code>NodeModel</code> which are retrieved from the filter panel.
     * @param settings - the node settings to write into
     * @throws InvalidSettingsException if one of the settings is not valid
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings)
            throws InvalidSettingsException {
        DataColumnSpecFilterConfiguration config = RaProjectionNodeModel.createDCSFilterConfiguration();
        m_filterPanel.saveConfiguration(config);
        config.saveConfiguration(settings);
    }
}

