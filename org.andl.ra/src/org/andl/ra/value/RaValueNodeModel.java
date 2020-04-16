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

package org.andl.ra.value;

import java.io.File;
import java.io.IOException;

import org.knime.core.data.DataTableSpec;
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

/**
 * <code>NodeModel</code> for the "RaValue" node.
 *
 * @author andl
 */
public class RaValueNodeModel extends NodeModel {
    
	private static final NodeLogger LOGGER = NodeLogger.getLogger(RaValueNodeModel.class);
	private static final String KEY_COLUMN_NAME = "column-name";
	private static final String KEY_TYPE_NAME = "column-type-name";
	private static final String KEY_EXPRESSION = "column-value-expression";
	private static final String DEFAULT_COLUMN_NAME = "new column";
	private static final String DEFAULT_TYPE_NAME = "STRING";
	private static final String DEFAULT_EXPRESSION = "";

	private final SettingsModelString _columnName = createSettingsColumnName();
	private final SettingsModelString _columnType = createSettingsColumnType();
	private final SettingsModelString _newExpression = createSettingsNewExpression();
	
	static SettingsModelString createSettingsColumnName() {
		return new SettingsModelString(KEY_COLUMN_NAME, DEFAULT_COLUMN_NAME);
	}
	
	static SettingsModelString createSettingsColumnType() {
		return new SettingsModelString(KEY_TYPE_NAME, DEFAULT_TYPE_NAME);
	}
	
	static SettingsModelString createSettingsNewExpression() {
		return new SettingsModelString(KEY_EXPRESSION, DEFAULT_EXPRESSION);
	}
	
	NewValueManager _newValueManager;
	
    //--------------------------------------------------------------------------
    // ctor and dummy overrides
	//
    protected RaValueNodeModel() {
        super(1, 1);
        LOGGER.info("Value node created");
    }

    /** {@inheritDoc} */
    @Override
    protected void reset() { }
    
    /** {@inheritDoc} */
    @Override
    protected void loadInternals(final File internDir,
            final ExecutionMonitor exec) throws IOException,
            CanceledExecutionException { }
    
    /** {@inheritDoc} */
    @Override
    protected void saveInternals(final File internDir,
            final ExecutionMonitor exec) throws IOException,
            CanceledExecutionException { }

    //--------------------------------------------------------------------------
    // execute, configure, settings
    //
    /** {@inheritDoc} */
    @Override
    protected BufferedDataTable[] execute(final BufferedDataTable[] inData,
            final ExecutionContext exec) throws Exception {

    	return new BufferedDataTable[] {
    		_newValueManager.execute(inData[0], exec) 
    	};
    }

    /** {@inheritDoc} */
    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs)
    throws InvalidSettingsException {

    	_newValueManager = new NewValueManager(inSpecs[0], _columnName.getStringValue(),
    		_columnType.getStringValue(), _newExpression.getStringValue());
        return new DataTableSpec[] { 
        	_newValueManager.getTableSpec() 
        };
    }
    
    /** {@inheritDoc} */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
		_columnName.saveSettingsTo(settings);
		_columnType.saveSettingsTo(settings);
		_newExpression.saveSettingsTo(settings);
    }

    /** {@inheritDoc} */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {
		_columnName.loadSettingsFrom(settings);
		_columnType.loadSettingsFrom(settings);
		_newExpression.loadSettingsFrom(settings);
    }

    /** {@inheritDoc} */
    @Override
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {
		_columnName.validateSettings(settings);
		_columnType.validateSettings(settings);
		_newExpression.validateSettings(settings);
    }
}

