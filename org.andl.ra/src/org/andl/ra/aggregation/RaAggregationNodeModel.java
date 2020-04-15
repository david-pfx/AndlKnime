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

package org.andl.ra.aggregation;

import java.io.File;
import java.io.IOException;
import java.util.List;

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
import org.knime.core.node.defaultnodesettings.SettingsModelFilterString;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.util.Pair;

/**
 * <code>NodeModel</code> for the "RaAggregation" node.
 *
 * @author Andl
 */
public class RaAggregationNodeModel extends NodeModel {
    
	private static final NodeLogger LOGGER = NodeLogger.getLogger(RaAggregationNodeModel.class);
	private static final String KEY_COLUMN_SELECTOR = "column-selector";
	private static final String KEY_OLD_COLUMN_NAME = "old-column-name";
	private static final String KEY_NEW_COLUMN_NAME = "new-column-name";
	private static final String KEY_AGG_FUNCTION = "agg-function";

	private final SettingsModelFilterString _columnFilter = createColumnFilter();
	private final SettingsModelString _unaggColumn = createUnaggColumn();
	private final SettingsModelString _aggColumn = createAggColumn();
	private final SettingsModelString _aggFunction = createAggFunction();

	private AggManager _aggManager;

	static SettingsModelFilterString createColumnFilter() {
		return new SettingsModelFilterString(KEY_COLUMN_SELECTOR);
	}

	static SettingsModelString createUnaggColumn() {
		return new SettingsModelString(KEY_OLD_COLUMN_NAME, "");
	}

	static SettingsModelString createAggColumn() {
		return new SettingsModelString(KEY_NEW_COLUMN_NAME, "");
	}

	static SettingsModelString createAggFunction() {
		return new SettingsModelString(KEY_AGG_FUNCTION, "");
	}
	
    //--------------------------------------------------------------------------
    // ctor and dummy overrides
	
    /**
     * Default constructor is all that is needed<br>
     * 
     * Aggregation uses a standard node model and replaces column(s) with a value 
     * created by evaluating an expression.
     */
	
    protected RaAggregationNodeModel() {
        super(1, 1);
        LOGGER.info("*ctor node created");
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

        LOGGER.info("*execute data=" + inData[0]);
        return new BufferedDataTable[] { 
        	_aggManager.execute(inData[0], exec) 
        };
    }

    /** {@inheritDoc} */
    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs)
    throws InvalidSettingsException {

        LOGGER.info("*config specs=" + inSpecs[0]);
        
        Pair<Boolean, String> check = checkSettings();
        if (!check.getFirst())
        	throw new InvalidSettingsException(check.getSecond());
        
        _aggManager = new AggManager(inSpecs[0], 
                _columnFilter.getExcludeList().toArray(new String[0]),
                _unaggColumn.getStringValue(),
                _aggColumn.getStringValue(),
                _aggFunction.getStringValue());
        return new DataTableSpec[] { 
       		_aggManager.getOutputSpec()
        };
    }

    /** {@inheritDoc} */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        LOGGER.debug("*save to settings=" + settings);
        
        // might not be valid but who cares
        _columnFilter.saveSettingsTo(settings);
    	_unaggColumn.saveSettingsTo(settings);
    	_aggColumn.saveSettingsTo(settings);
    	_aggFunction.saveSettingsTo(settings);
    }

    /** {@inheritDoc} */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        LOGGER.debug("*load from settings=" + settings);

        _columnFilter.loadSettingsFrom(settings);
    	_unaggColumn.loadSettingsFrom(settings);
    	_aggColumn.loadSettingsFrom(settings);
    	_aggFunction.loadSettingsFrom(settings);
    }

    /** {@inheritDoc} */
    @Override
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        LOGGER.debug("*validate settings=" + settings);
        
        _columnFilter.validateSettings(settings);
    	_unaggColumn.validateSettings(settings);
    	_aggColumn.validateSettings(settings);
    	_aggFunction.validateSettings(settings);
    }
    
    //==========================================================================
    // implementation
    //
    
    // Carry out tests to see if settings are valid
    Pair<Boolean, String> checkSettings() {
        List<String> groupCols =_columnFilter.getExcludeList(); 
        List<String> availCols =_columnFilter.getIncludeList(); 
        if (availCols.isEmpty())
        	return Pair.create(false, "no columns available for aggregation");
        
		String unaggCol = _unaggColumn.getStringValue();
		if (!availCols.contains(unaggCol))
			return Pair.create(false, "invalid column for aggregation: " + unaggCol);

    	String aggFunc = _aggFunction.getStringValue();
    	List<String> availFuncs = AggFunction.getAllNames(); //TODO: filter by type
		if (!availFuncs.contains(aggFunc))
			return Pair.create(false, "not a valid aggregating function: " + aggFunc);

    	String aggCol = _aggColumn.getStringValue();
    	if (" ".compareTo(aggCol) >= 0 || groupCols.contains(aggCol))
    		return Pair.create(false, "not a valid aggregated column name: " + aggCol);
    	// ok
    	return Pair.create(true, null);
    }
}
