package org.andl.ra.aggregation;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;

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
import org.knime.core.node.defaultnodesettings.SettingsModelFilterString;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * <code>NodeModel</code> for the "RaAggregation" node.
 *
 * @author Andl
 */
public class RaAggregationNodeModel extends NodeModel {
    
	private static final NodeLogger LOGGER = NodeLogger.getLogger(RaValueNodeModel.class);
	private static final String KEY_COLUMN_SELECTOR = "column-selector";
	private static final String KEY_NEW_COLUMN_NAMES = "new-column-names";
	private static final String KEY_EXPRESSIONS = "expressions";

	private final SettingsModelFilterString _columnFilterSettings = createSettingsColumnFilter();
	private final SettingsModelString[] _newColumnNameSettings = createSettingsNewColumnNames(0);
	private final SettingsModelString[] _newExpressionsSettings = createSettingsExpressions(0);

	static SettingsModelFilterString createSettingsColumnFilter() {
		return new SettingsModelFilterString(KEY_COLUMN_SELECTOR);
	}

	// get settings model for new column names
	static SettingsModelString[] createSettingsNewColumnNames(int noCols) {
		SettingsModelString[] settings = new SettingsModelString[noCols];
		for (int i = 0; i < noCols; ++i)
			settings[i] = new SettingsModelString(KEY_NEW_COLUMN_NAMES + i, "");
		return settings;
	}
	
	// get settings model for new column names
	static SettingsModelString[] createSettingsExpressions(int noCols) {
		SettingsModelString[] settings = new SettingsModelString[noCols];
		for (int i = 0; i < noCols; ++i)
			settings[i] = new SettingsModelString(KEY_EXPRESSIONS + i, "");
		return settings;
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
        LOGGER.info("node created");
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

        LOGGER.info("execute " + inData[0]);
        return new BufferedDataTable[] { 
        	doAggregation(inData[0], exec) 
        };
    }

    /** {@inheritDoc} */
    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs)
    throws InvalidSettingsException {

        LOGGER.info("config " + inSpecs[0]);
        ColumnRearranger c = createColumnRearranger(inSpecs[0]);
        return new DataTableSpec[] { 
        	c.createSpec() 
        };
    }

    /** {@inheritDoc} */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
    	for (SettingsModelString model : _newColumnNameSettings)
    		model.saveSettingsTo(settings);
    	for (SettingsModelString model : _newExpressionsSettings)
    		model.saveSettingsTo(settings);
    }

    /** {@inheritDoc} */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {
    	for (SettingsModelString model : _newColumnNameSettings)
    		model.loadSettingsFrom(settings);
    	for (SettingsModelString model : _newExpressionsSettings)
    		model.loadSettingsFrom(settings);
    }

    /** {@inheritDoc} */
    @Override
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {
    	for (SettingsModelString model : _newColumnNameSettings)
    		model.validateSettings(settings);
    	for (SettingsModelString model : _newExpressionsSettings)
    		model.validateSettings(settings);
    }
    
    //==========================================================================

    // create column rearranger 
    ColumnRearranger createColumnRearranger(final DataTableSpec spec) {
        final List<String> incls = _columnFilterSettings.getIncludeList();
        final ColumnRearranger c = new ColumnRearranger(spec);
        c.keepOnly(incls.toArray(new String[incls.size()]));
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

