package org.andl.ra.projection;

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.IllegalFormatException;
import java.util.List;
import org.andl.ra.set.RaTuple;
import org.knime.base.node.preproc.filter.row.RowFilterIterator;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.RowIterator;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.container.ColumnRearranger;
import org.knime.core.data.container.JoinedTable;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.streamable.InputPortRole;
import org.knime.core.node.streamable.OutputPortRole;
import org.knime.core.node.streamable.PartitionInfo;
import org.knime.core.node.streamable.StreamableOperator;
import org.knime.core.node.util.filter.NameFilterConfiguration.FilterResult;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterConfiguration;


/**
 * Implements the model for the "RaProjection" node.
 *
 * Include the columns comprising a projection subset.
 * Copy across required columns of input rows.
 * Remove duplicates from output.
 *
 * @author andl
 */
public class RaProjectionNodeModel extends NodeModel {
	/**
	 * The settings key to retrieve and store settings shared between node dialog
	 * and node model. 
	 */
	private static final String KEY_PROJECTION_COLUMNS = "projection-columns";
	
	/** 
	 * Configuration representing a column selection
	 */
    private DataColumnSpecFilterConfiguration m_conf;

    /** Creates a new projection model with one each input and output. */
    public RaProjectionNodeModel() {
        super(1, 1);
    }

    /** Constructor for in and out <code>PortType</code> objects.
     * @param inPorts - in ports
     * @param outPorts - out ports
     */
    public RaProjectionNodeModel(final PortType[] inPorts, final PortType[] outPorts) {
        super(inPorts, outPorts);
    }

    /** {@inheritDoc} */
    @Override
    protected void reset() { }

    /** {@inheritDoc} */
    @Override
    protected BufferedDataTable[] execute(final BufferedDataTable[] data, final ExecutionContext exec) 
    throws Exception {
        ColumnRearranger c = createColumnRearranger(data[0].getDataTableSpec());
        BufferedDataTable tempTable = exec.createColumnRearrangeTable(data[0], c, exec);
        BufferedDataContainer container =
            exec.createDataContainer(tempTable.getDataTableSpec());
        
        exec.setMessage("Searching first matching row...");
        int count = 0;
        HashSet<RaTuple> tupleSet = new HashSet<RaTuple>();
        RowIterator iter = tempTable.iterator();
        try {
            while (iter.hasNext()) {
                DataRow row = iter.next();
                RaTuple tuple = new RaTuple(row);
            	if (!tupleSet.contains(tuple)) {
	                count++;
	                tupleSet.add(tuple);
	                container.addRowToTable(row);
	                exec.setMessage("Added row " + count + " (\""
	                        + row.getKey() + "\")");
            	}
            }
        } catch (RowFilterIterator.RuntimeCanceledExecutionException rce) {
            throw rce.getCause();
        } finally {
            container.close();
        }
        return new BufferedDataTable[] { container.getTable() };
    }

    /** {@inheritDoc} */
    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec) 
    throws IOException, CanceledExecutionException { }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec) 
    throws IOException, CanceledExecutionException { }

    /**
     * Excludes a number of columns from the input spec and generates a new
     * output spec.
     *
     * @param inSpecs - input table specification
     * @return outSpecs - output table specification with only the projected columns
     * @throws InvalidSettingsException if a selected column is not available.
     */
    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs)
    throws InvalidSettingsException {
        ColumnRearranger c = createColumnRearranger(inSpecs[0]);
        return new DataTableSpec[]{c.createSpec()};
    }

    /**
     * Creates the output data table spec according to the current settings.
     * Throws an InvalidSettingsException if columns are specified that don't
     * exist in the input table spec.
     *
     * @since 3.1
     */
    protected ColumnRearranger createColumnRearranger(final DataTableSpec spec) {
        if (m_conf == null) {
            m_conf = createDCSFilterConfiguration();
            // auto-configure
            m_conf.loadDefaults(spec, true);
        }
        final FilterResult filter = getFilterResult(spec);
        final String[] incls = filter.getIncludes();
        final ColumnRearranger c = new ColumnRearranger(spec);
        c.keepOnly(incls);
        return c;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamableOperator createStreamableOperator(final PartitionInfo partitionInfo, final PortObjectSpec[] inSpecs)
        throws InvalidSettingsException {
        return createColumnRearranger((DataTableSpec) inSpecs[0]).createStreamableFunction();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputPortRole[] getInputPortRoles() {
        return new InputPortRole[]{InputPortRole.DISTRIBUTED_STREAMABLE};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OutputPortRole[] getOutputPortRoles() {
        return new OutputPortRole[]{OutputPortRole.DISTRIBUTED};
    }

    /**
     * Writes number of filtered columns, and the names as
     * {@link org.knime.core.data.DataCell} to the given settings.
     *
     * @param settings the object to save the settings into
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        if (m_conf != null) {
            m_conf.saveConfiguration(settings);
        }
    }

    /**
     * Reads the filtered columns.
     *
     * @param settings to read from
     * @throws InvalidSettingsException if the settings does not contain the
     *             size or a particular column key
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        DataColumnSpecFilterConfiguration conf = createDCSFilterConfiguration();
        conf.loadConfigurationInModel(settings);
        m_conf = conf;
    }

    /** {@inheritDoc} */
    @Override
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        DataColumnSpecFilterConfiguration conf = createDCSFilterConfiguration();
        conf.loadConfigurationInModel(settings);
    }

    /** A new configuration to store the settings. Also enables the type filter.
     * @return ...
     */
    static final  DataColumnSpecFilterConfiguration createDCSFilterConfiguration() {
        return new DataColumnSpecFilterConfiguration(KEY_PROJECTION_COLUMNS);
    }

    /** Returns the object holding the include and exclude columns.
     * @param spec - the spec to be applied to the current configuration.
     * @return filter result
     */
    protected FilterResult getFilterResult(final DataTableSpec spec) {
        return (m_conf == null) ? null : m_conf.applyTo(spec);
    }
}

