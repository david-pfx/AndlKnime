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

package org.andl.ra.join;

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
 * This is an example implementation of the node model of the
 * "RaJoin" node.
 * 
 * The following join operations are supported. All joins are based on common 
 * columns matched by name and type.
 * 
 * Join is the natural join. The output includes all columns from both inputs.
 * Compose is Join with the join columns removed from the output.
 * Semijoin is the left (top) input with only rows that DO match the right (bottom) input included.
 * Antijoin is the left (top) input with only rows that DO NOT match the right (bottom) input included.
 * 
 * @author andl
 */
public class RaJoinNodeModel extends NodeModel {
    
	private static final NodeLogger LOGGER = NodeLogger.getLogger(RaJoinNodeModel.class);
	private static final String KEY_OPERATION = "join-operation";
	private static final String DEFAULT_OPERATION = "Join";
	static final String[] ALL_OPERATIONS = {
		"Join", "Semijoin", "Antijoin"
	};
	
	private final SettingsModelString _joinOperationSettings = createSettingsModel();
	private JoinManager _joinMgr;

	// create settings model for choice of operation (shared with dialog)
	static SettingsModelString createSettingsModel() {
		return new SettingsModelString(KEY_OPERATION, DEFAULT_OPERATION);
	}

	// ctor
	protected RaJoinNodeModel() {
		super(2, 1);
		LOGGER.info("created");
	}

	/** {@inheritDoc} */
	@Override
	protected BufferedDataTable[] execute(final BufferedDataTable[] inData, final ExecutionContext exec)
	throws Exception {

    	return new BufferedDataTable[] {
        	_joinMgr.execute(inData, exec) 
        };
	}
	
	/** {@inheritDoc} */
	@Override
	protected DataTableSpec[] configure(final DataTableSpec[] inSpecs) throws InvalidSettingsException {

		_joinMgr = new JoinManager(inSpecs, _joinOperationSettings.getStringValue());
        return new DataTableSpec[] { 
        	_joinMgr.getTableSpec() 
        };
	}

	/** {@inheritDoc} */
	@Override
	protected void saveSettingsTo(final NodeSettingsWO settings) {
		_joinOperationSettings.saveSettingsTo(settings);
	}

	/** {@inheritDoc} */
	@Override
	protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
		_joinOperationSettings.loadSettingsFrom(settings);
	}

	/** {@inheritDoc} */
	@Override
	protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		_joinOperationSettings.validateSettings(settings);
	}

	/** {@inheritDoc} */
	@Override
	protected void loadInternals(File nodeInternDir, ExecutionMonitor exec)
	throws IOException, CanceledExecutionException { }

	/** {@inheritDoc} */
	@Override
	protected void saveInternals(File nodeInternDir, ExecutionMonitor exec)
	throws IOException, CanceledExecutionException { }

	/** {@inheritDoc} */
	@Override
	protected void reset() { }
}

