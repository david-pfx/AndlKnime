package org.andl.ra.aggregation;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;

import org.andl.ra.RaTuple;
import org.andl.ra.value.RaValueNodeModel;
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
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelStringArray;
import org.knime.core.node.util.filter.NameFilterConfiguration.FilterResult;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterConfiguration;

/**
 * <code>NodeModel</code> for the "RaAggregation" node.
 *
 * @author Andl
 */
public class RaAggregationNodeModel extends NodeModel {
    
	private static final NodeLogger LOGGER = NodeLogger.getLogger(RaValueNodeModel.class);
	private static final String KEY_PROJECTION_COLUMNS = "projection-columns";
//	private static final String KEY_OLD_COLUMN_NAMES = "old-column-names";
	private static final String KEY_NEW_COLUMN_NAMES = "new-column-names";
//	private static final String KEY_NEW_COLUMN_TYPES = "new-column-types";
	private static final String KEY_EXPRESSIONS = "expressions";

//	private final SettingsModelStringArray _oldColumnNameSettings = createSettingsOldColumnNames();
//	private final SettingsModelStringArray _newColumnNameSettings = createSettingsNewColumnNames();
	private final SettingsModelStringArray _newColumnNameSettings = createSettingsNewColumnNames();
	private final SettingsModelStringArray _newExpressionsSettings = createSettingsExpressions();

    private DataColumnSpecFilterConfiguration m_conf;

    // create new configuration object to drive selection panel
    static final  DataColumnSpecFilterConfiguration createDCSFilterConfiguration() {
    	// disable selection by pattern and type 
        return new DataColumnSpecFilterConfiguration(KEY_PROJECTION_COLUMNS, null, 0);
    }

//    // get settings model for old column names
//	static SettingsModelStringArray createSettingsOldColumnNames() {
//		return new SettingsModelStringArray(KEY_OLD_COLUMN_NAMES, new String[0]);
//	}
//
//	// get settings model for new column names
//	static SettingsModelStringArray createSettingsNewColumnNames() {
//		return new SettingsModelStringArray(KEY_NEW_COLUMN_NAMES, new String[0]);
//	}
//	
	// get settings model for new column names
	static SettingsModelStringArray createSettingsNewColumnNames() {
		return new SettingsModelStringArray(KEY_NEW_COLUMN_NAMES, new String[0]);
	}
	
	// get settings model for new column names
	static SettingsModelStringArray createSettingsExpressions() {
		return new SettingsModelStringArray(KEY_EXPRESSIONS, new String[0]);
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
        LOGGER.info("Aggregation node created");
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
        	doAggregation(inData[0], exec) 
        };
    }

    /** {@inheritDoc} */
    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs)
    throws InvalidSettingsException {

        ColumnRearranger c = createColumnRearranger(inSpecs[0]);
        return new DataTableSpec[] { 
        	c.createSpec() 
        };
    }

    /** {@inheritDoc} */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
//		_oldColumnNameSettings.saveSettingsTo(settings);
//		_newColumnNameSettings.saveSettingsTo(settings);
		_newColumnNameSettings.saveSettingsTo(settings);
		_newExpressionsSettings.saveSettingsTo(settings);
    }

    /** {@inheritDoc} */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {
//		_oldColumnNameSettings.loadSettingsFrom(settings);
//		_newColumnNameSettings.loadSettingsFrom(settings);
		_newColumnNameSettings.loadSettingsFrom(settings);
		_newExpressionsSettings.loadSettingsFrom(settings);
    }

    /** {@inheritDoc} */
    @Override
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {
//		_oldColumnNameSettings.validateSettings(settings);
//		_newColumnNameSettings.validateSettings(settings);
		_newColumnNameSettings.validateSettings(settings);
		_newExpressionsSettings.validateSettings(settings);
    }
    
    //==========================================================================

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
	private BufferedDataTable doAggregation(final BufferedDataTable inData, final ExecutionContext exec) 
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

