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

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
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
    private DataColumnSpecFilterConfiguration _columnConfig;
	private ProjectionManager _projectionManager;

    // create new configuration object to drive selection panel
    static final  DataColumnSpecFilterConfiguration createDCSFilterConfiguration() {
    	// disable selection by pattern and type 
        return new DataColumnSpecFilterConfiguration(KEY_PROJECTION_COLUMNS, null, 0);
    }

    //--------------------------------------------------------------------------
    // ctor and dummy overrides
    public RaProjectionNodeModel() {
        super(1, 1);
    }

    /** {@inheritDoc} */
    @Override
    protected void reset() { }

    /** {@inheritDoc} */
    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec) 
    throws IOException, CanceledExecutionException { }

    /** {@inheritDoc} */
    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec) 
    throws IOException, CanceledExecutionException { }

    //--------------------------------------------------------------------------
    // execute, configure, settings
    //
    // project creates a new table using a container
    //
    /** {@inheritDoc} */
    @Override
    protected BufferedDataTable[] execute(final BufferedDataTable[] inData, final ExecutionContext exec) 
    throws Exception {
    	
        return new BufferedDataTable[] {
        	_projectionManager.execute(inData[0], exec) 
        };
    }

    /** {@inheritDoc} */
    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs)
    throws InvalidSettingsException {
    	
    	if (_columnConfig == null) {
    		_columnConfig = createDCSFilterConfiguration();
    		_columnConfig.loadDefaults(inSpecs[0], true);
    	}
    	_projectionManager = new ProjectionManager(inSpecs[0], _columnConfig);
    	return new DataTableSpec[] { 
    		_projectionManager.getTableSpec() 
    	};
    }

    /** {@inheritDoc} */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        if (_columnConfig != null)
            _columnConfig.saveConfiguration(settings);
    }

    /** {@inheritDoc} */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        DataColumnSpecFilterConfiguration conf = createDCSFilterConfiguration();
        conf.loadConfigurationInModel(settings);
        _columnConfig = conf;
    }

    /** {@inheritDoc} */
    @Override
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        DataColumnSpecFilterConfiguration conf = createDCSFilterConfiguration();
        conf.loadConfigurationInModel(settings);
    }
}
