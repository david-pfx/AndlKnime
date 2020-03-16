package org.andl.ra.extension;

import java.io.File;
import java.io.IOException;

import org.andl.ra.RaEvaluator;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.CellFactory;
import org.knime.core.data.container.ColumnRearranger;
import org.knime.core.data.container.SingleCellFactory;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.streamable.simple.SimpleStreamableFunctionNodeModel;

/**
 * <code>NodeModel</code> for the "RaExtension" node.
 *
 * @author andl
 */
public class RaExtensionNodeModel extends SimpleStreamableFunctionNodeModel {
    
	private static final NodeLogger LOGGER = NodeLogger.getLogger(RaExtensionNodeModel.class);
	private static final String KEY_COLUMN_NAME = "column-name";
	private static final String KEY_TYPE_NAME = "column-type-name";
	private static final String KEY_EXPRESSION = "column-value-expression";
	private static final String DEFAULT_COLUMN_NAME = "new column";
	private static final String DEFAULT_TYPE_NAME = "STRING";
	private static final String DEFAULT_EXPRESSION = "";

	private final SettingsModelString _columnNameSettings = createSettingsColumnName();
	private final SettingsModelString _columnTypeNameSettings = createSettingsColumnTypeName();
	private final SettingsModelString _expressionSettings = createSettingsExpression();
	
    /**
     * Constructor for the node model.
     */
    protected RaExtensionNodeModel() {
        super();
        LOGGER.info("Extension node created");
    }

	static SettingsModelString createSettingsColumnName() {
		return new SettingsModelString(KEY_COLUMN_NAME, DEFAULT_COLUMN_NAME);
	}

	static SettingsModelString createSettingsColumnTypeName() {
		return new SettingsModelString(KEY_TYPE_NAME, DEFAULT_TYPE_NAME);
	}

	static SettingsModelString createSettingsExpression() {
		return new SettingsModelString(KEY_EXPRESSION, DEFAULT_EXPRESSION);
	}

    /** {@inheritDoc} */
    @Override
    protected BufferedDataTable[] execute(final BufferedDataTable[] inData,
            final ExecutionContext exec) throws Exception {

        DataTableSpec spec = inData[0].getDataTableSpec();
        ColumnRearranger rearranger = createColumnRearranger(spec);
        BufferedDataTable out = exec.createColumnRearrangeTable(inData[0], rearranger, exec);
        return new BufferedDataTable[] { out };
    }

    /** {@inheritDoc} */
    @Override
    protected void reset() { }

    /** {@inheritDoc} */
    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs)
    throws InvalidSettingsException {
    	
        ColumnRearranger rearranger = createColumnRearranger(inSpecs[0]);
        DataTableSpec out = rearranger.createSpec();
        return new DataTableSpec[] { out };
    }
    
    /** {@inheritDoc} */
    @Override
    protected ColumnRearranger createColumnRearranger(final DataTableSpec inspec) throws InvalidSettingsException {
        String colname = _columnNameSettings.getStringValue();
        String typename = _columnTypeNameSettings.getStringValue();
        String expression = _expressionSettings.getStringValue();
        TypeCellFactory tcf = TypeCellFactory.valueOf(typename); 

		try {
	        DataColumnSpec outcolspec = new DataColumnSpecCreator(colname, tcf.getDataType()).createSpec();
	        RaEvaluator jexl = new RaEvaluator(inspec, outcolspec.getType(), expression); 
	
	        ColumnRearranger rearranger = new ColumnRearranger(inspec);
	        CellFactory fac = new SingleCellFactory(outcolspec) {
	            @Override
	            public DataCell getCell(final DataRow row) {
	            	return jexl.evaluateDataCell(row);
	            }
	        };
	
	        rearranger.append(fac);
	        return rearranger;
		} catch (Exception e) {
			throw new InvalidSettingsException(
				"Not a valid expression for the type: " + e.getMessage(), e);
		}
    }
    
    /** {@inheritDoc} */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
		_columnNameSettings.saveSettingsTo(settings);
		_columnTypeNameSettings.saveSettingsTo(settings);
		_expressionSettings.saveSettingsTo(settings);
    }

    /** {@inheritDoc} */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {
		_columnNameSettings.loadSettingsFrom(settings);
		_columnTypeNameSettings.loadSettingsFrom(settings);
		_expressionSettings.loadSettingsFrom(settings);
    }

    /** {@inheritDoc} */
    @Override
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {
		_columnNameSettings.validateSettings(settings);
		_columnTypeNameSettings.validateSettings(settings);
		_expressionSettings.validateSettings(settings);
    }
    
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


}

