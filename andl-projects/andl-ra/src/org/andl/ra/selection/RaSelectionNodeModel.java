package org.andl.ra.selection;

import java.io.File;
import java.io.IOException;

import org.andl.ra.RaEvaluator;
import org.knime.base.node.preproc.filter.row.RowFilterIterator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTable;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.RowIterator;
import org.knime.core.data.def.BooleanCell;
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

/**
 * <code>NodeModel</code> for the "RaSelection" node.
 *
 * @author David Bennett --- Andl
 */
public class RaSelectionNodeModel extends NodeModel {
	
	private static final NodeLogger LOGGER = NodeLogger.getLogger(RaSelectionNodeModel.class);
	private static final String KEY_EXPRESSION = "column-value-expression";
	private static final String DEFAULT_EXPRESSION = "";

	private final SettingsModelString _expressionSettings = createSettingsExpression();
	private RaEvaluator _evaluator;
	
	static SettingsModelString createSettingsExpression() {
		return new SettingsModelString(KEY_EXPRESSION, DEFAULT_EXPRESSION);
	}

    //--------------------------------------------------------------------------
    // ctor and dummy overrides
    protected RaSelectionNodeModel() {
        super(1, 1);
        LOGGER.info("Selection node created");
    }

    /* {@inheritDoc} */
    @Override
    protected void reset() { }
    
    @Override
    protected void loadInternals(final File internDir,
            final ExecutionMonitor exec) throws IOException,
            CanceledExecutionException { }
    
    @Override
    protected void saveInternals(final File internDir,
            final ExecutionMonitor exec) throws IOException,
            CanceledExecutionException { }

	//--------------------------------------------------------------------------
    // execute, configure, settings
    //
    // selection creates a new table using a container and a boolean evaluator
    //
    @Override
    protected BufferedDataTable[] execute(final BufferedDataTable[] inData, final ExecutionContext exec) 
    throws Exception {

        return new BufferedDataTable[] { 
        	doSelection(inData[0], exec) 
        };
    }

    /* {@inheritDoc} */
    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs)
    throws InvalidSettingsException {

        _evaluator = new RaEvaluator(inSpecs[0], BooleanCell.TYPE, _expressionSettings.getStringValue());
        return inSpecs;
    }

    /* {@inheritDoc} */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
		_expressionSettings.saveSettingsTo(settings);
    }

    /* {@inheritDoc} */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
    throws InvalidSettingsException {
		_expressionSettings.loadSettingsFrom(settings);
    }

    /* {@inheritDoc} */
    @Override
    protected void validateSettings(final NodeSettingsRO settings)
    throws InvalidSettingsException {
		_expressionSettings.validateSettings(settings);
    }
    
    //==========================================================================
    
    // implement selection operation
    // uses row iterator and boolean expression evaluator
    
	private BufferedDataTable doSelection(DataTable intable, final ExecutionContext exec)
	throws CanceledExecutionException {
		
		DataTableSpec dspec = intable.getDataTableSpec();
        BufferedDataContainer container = exec.createDataContainer(dspec);
        
        exec.setMessage("Searching first matching row...");
        int count = 0;
        RowIterator iter = intable.iterator();
        try {
            while (iter.hasNext()) {
                DataRow row = iter.next();
                if (_evaluator.evaluateBoolean(row)) {
	                count++;
	                container.addRowToTable(row);
	                exec.setMessage("Added row " + count);
            	}
            }
        } catch (RowFilterIterator.RuntimeCanceledExecutionException rce) {
            throw rce.getCause();
        } finally {
            container.close();
        }
        BufferedDataTable outtable = container.getTable();
		return outtable;
	}
}

