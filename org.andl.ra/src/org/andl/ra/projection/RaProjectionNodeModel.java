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

package org.andl.ra.projection;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;

import org.andl.ra.RaTuple;
import org.knime.base.node.preproc.filter.row.RowFilterIterator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.RowIterator;
import org.knime.core.data.container.ColumnRearranger;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObjectSpec;
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
	// The settings key
	private static final String KEY_PROJECTION_COLUMNS = "projection-columns";
	// Represent a column selection
    private DataColumnSpecFilterConfiguration m_conf;

    // create new configuration object to drive selection panel
    static final  DataColumnSpecFilterConfiguration createDCSFilterConfiguration() {
    	// disable selection by pattern and type 
        return new DataColumnSpecFilterConfiguration(KEY_PROJECTION_COLUMNS, null, 0);
    }

    public RaProjectionNodeModel() {
        super(1, 1);
    }

    /** {@inheritDoc} */
    @Override
    protected void reset() { }

    /** {@inheritDoc} */
    @Override
    protected BufferedDataTable[] execute(final BufferedDataTable[] inData, final ExecutionContext exec) 
    throws Exception {
        return new BufferedDataTable[] { doProjection(inData[0], exec) };
    }

    /** {@inheritDoc} */
    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec) 
    throws IOException, CanceledExecutionException { }

    /** {@inheritDoc} */
    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec) 
    throws IOException, CanceledExecutionException { }

    /** {@inheritDoc} 
     * Just return the new spec
     * */
    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs)
    throws InvalidSettingsException {
    	for (String name : inSpecs[0].getColumnNames()) 
    		if (name.charAt(0) <= ' ')
    			throw new InvalidSettingsException("bad column name: " + name);
        ColumnRearranger c = createColumnRearranger(inSpecs[0]);
        return new DataTableSpec[] { c.createSpec() };
    }

    /** {@inheritDoc} */
    @Override
    public StreamableOperator createStreamableOperator(final PartitionInfo partitionInfo, final PortObjectSpec[] inSpecs)
        throws InvalidSettingsException {
        return createColumnRearranger((DataTableSpec) inSpecs[0]).createStreamableFunction();
    }

    /** {@inheritDoc} */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        if (m_conf != null)
            m_conf.saveConfiguration(settings);
    }

    /** {@inheritDoc} */
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
    
    //==========================================================================

    // create column rearranger 
    ColumnRearranger createColumnRearranger(final DataTableSpec spec) {
        if (m_conf == null) {
            m_conf = createDCSFilterConfiguration();
            m_conf.loadDefaults(spec, true);
        }
        final FilterResult filter = m_conf.applyTo(spec);
        final String[] incls = filter.getIncludes();
        final ColumnRearranger c = new ColumnRearranger(spec);
        c.keepOnly(incls);
        return c;
    }

    // implement projection algorithm
	private BufferedDataTable doProjection(final BufferedDataTable inData, final ExecutionContext exec) 
	throws CanceledExecutionException {

        ColumnRearranger colre = createColumnRearranger(inData.getDataTableSpec());
		BufferedDataTable tempTable = exec.createColumnRearrangeTable(inData, colre, exec);
        BufferedDataContainer container = exec.createDataContainer(tempTable.getDataTableSpec());
        
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
	                exec.setMessage("Added row " + count);
            	}
            }
        } catch (RowFilterIterator.RuntimeCanceledExecutionException rce) {
            throw rce.getCause();
        } finally {
            container.close();
        }
		return container.getTable();
	}

}

