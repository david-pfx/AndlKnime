package org.andl.ra;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DataTypeRegistry;
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
 * <code>NodeModel</code> for the "RaExtension" node.
 *
 * @author andl
 */
public class RaExtensionNodeModel extends NodeModel {
    
	private static final NodeLogger LOGGER = NodeLogger.getLogger(RaExtensionNodeModel.class);
	private static final String KEY_NAME = "column-name";
	private static final String KEY_TYPE = "column-type";
	private static final String KEY_EXPRESSION = "column-expression";
	private static final String DEFAULT_TYPE = "string";
	static final String[] ALL_TYPES = {
		"sring", "int", "double", "date"
	};

	private final SettingsModelString _nameSettings = createSettingsColumnName();
	private final SettingsModelString _typeSettings = createSettingsColumnType();
	private final SettingsModelString _expressionSettings = createSettingsExpression();
	
    /**
     * Constructor for the node model.
     */
    protected RaExtensionNodeModel() {
        super(1, 1);
        LOGGER.info("Extension node created");
    }

	static SettingsModelString createSettingsColumnName() {
		return new SettingsModelString(KEY_NAME, "new column");
	}

	static SettingsModelString createSettingsColumnType() {
		return new SettingsModelString(KEY_TYPE, "string");
	}

	static SettingsModelString createSettingsExpression() {
		return new SettingsModelString(KEY_EXPRESSION, "");
	}

    /**
     * {@inheritDoc}
     */
    @Override
    protected BufferedDataTable[] execute(final BufferedDataTable[] inData,
            final ExecutionContext exec) throws Exception {

        // TODO: Return a BufferedDataTable for each output port 
        return new BufferedDataTable[]{};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        // TODO: generated method stub
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs)
    throws InvalidSettingsException {
    	
//    	DataType newtype = DataTypeRegistry 
//    	DataColumnSpec newcol = DataColumnSpecCreator(
//    			_nameSettings.getStringValue(), _);
//    	List<DataColumnSpec> specs = new ArrayList<>();
//    	specs.addAll(inSpecs[0].stream());
//    	specs.add(e)
    	DataTableSpec newspec = inSpecs[0];

        // TODO: generated method stub
        return new DataTableSpec[]{ newspec};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
         // TODO: generated method stub
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        // TODO: generated method stub
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        // TODO: generated method stub
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File internDir,
            final ExecutionMonitor exec) throws IOException,
            CanceledExecutionException {
        // TODO: generated method stub
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File internDir,
            final ExecutionMonitor exec) throws IOException,
            CanceledExecutionException {
        // TODO: generated method stub
    }


}

