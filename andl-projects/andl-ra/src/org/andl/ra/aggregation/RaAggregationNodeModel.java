package org.andl.ra.aggregation;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import org.andl.ra.RaTuple;
import org.knime.base.node.preproc.filter.row.RowFilterIterator;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowIterator;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DefaultRow;
//import org.knime.core.data.def.DateAndTimeCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.StringCell;
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
    
	private static final NodeLogger LOGGER = NodeLogger.getLogger(RaAggregationNodeModel.class);
	private static final String KEY_COLUMN_SELECTOR = "column-selector";
	private static final String KEY_NEW_COLUMN_NAMES = "new-column-names";
	private static final String KEY_EXPRESSIONS = "expressions";

	private final SettingsModelFilterString _columnFilterSettings = createSettingsColumnFilter();
	private final SettingsModelString[] _newColumnNameSettings = createSettingsNewColumnNames(0);
	private final SettingsModelString[] _newExpressionsSettings = createSettingsExpressions(0);

	private AggManager _aggManager;

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
        	_aggManager.execute(inData[0], exec) 
        };
    }

    /** {@inheritDoc} */
    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs)
    throws InvalidSettingsException {

        LOGGER.info("config " + inSpecs[0]);
        _aggManager = new AggManager(inSpecs[0], 
                _columnFilterSettings.getIncludeList().toArray(new String[0]),
                _newColumnNameSettings[0].getStringValue(),
                _newColumnNameSettings[0].getStringValue(),
                _newExpressionsSettings[0].getStringValue());
        return new DataTableSpec[] { 
       		_aggManager.getOutputSpec()
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

    // implement aggregation algorithm
//	private BufferedDataTable doAggregation(final BufferedDataTable inData, final ExecutionContext exec) 
//	throws CanceledExecutionException {
//
//        ColumnRearranger colre = createColumnRearranger(inData.getDataTableSpec());
//		BufferedDataTable tempTable = exec.createColumnRearrangeTable(inData, colre, exec);
//        BufferedDataContainer container = exec.createDataContainer(tempTable.getDataTableSpec());
//        
//        
//        Hashtable<RaTuple, Accumulators> tupleHash = new Hashtable<>();
//        
//        RowIterator iter = tempTable.iterator();
//        exec.setMessage("Searching first matching row...");
//        int count = 0;
//        try {
//            while (iter.hasNext()) {
//                DataRow row = iter.next();
//                RaTuple tuple = new RaTuple(row);
//                count++;
//            	if (tupleHash.contains(tuple)) {
//            		tupleHash.get(tuple).accumulate(cells);
//            	} else {
//            		Accumulators accum = new Accumulators(functions, cells);
//	                tupleHash.put(tuple, accum);
////	                container.addRowToTable(row);
////	                exec.setMessage("Added row " + count);
//            	}
//            }
//        } catch (RowFilterIterator.RuntimeCanceledExecutionException rce) {
//            throw rce.getCause();
//        } finally {
//            container.close();
//        }
//		return container.getTable();
//	}
}

/*******************************************************************************
 *  
 * Functions and data to manage aggregation
 */
class AggManager {
	DataTableSpec _inputSpec, _outputSpec;
	int[] _groupColNos;
	int _argColNo;
	AggFunctions _function = null;

	DataTableSpec getOutputSpec() { return _outputSpec; }

	AggManager(DataTableSpec inputSpec, String[] groupCols, String argCol, String retCol, String aggFunction) 
	throws InvalidSettingsException {
		_inputSpec = inputSpec;
		_groupColNos = inputSpec.columnsToIndices(groupCols);
		_argColNo = inputSpec.findColumnIndex(argCol); 
		
		for (AggFunctions value : AggFunctions.values()) {
			if (value.getName().equals(aggFunction)) {
				_function = value;
				break;
			}
		}
		if (_function == null)
			throw new InvalidSettingsException("Unknown function: " + aggFunction);
		
		AggType argType = AggType.getAggType(_inputSpec.getColumnSpec(_argColNo).getType());
		if (argType == null)
			throw new InvalidSettingsException("Unsupported column type: " + aggFunction);
		
		AggType retType = _function.getReturnType(argType);
		
		ArrayList<DataColumnSpec> outcols = new ArrayList<>();
		for (int colx : _groupColNos)
			outcols.add(_inputSpec.getColumnSpec(colx));
		outcols.add(new DataColumnSpecCreator(retCol, retType.getDataType()).createSpec());
		_outputSpec = new DataTableSpec(outcols.toArray(new DataColumnSpec[0]));
		
	}

	// execute the aggregation algorithm
	BufferedDataTable execute(BufferedDataTable inData, ExecutionContext exec) 
	throws CanceledExecutionException {

		BufferedDataContainer container = exec.createDataContainer(_outputSpec);
		Hashtable<RaTuple, Accumulator> tupleHash = new Hashtable<>();
		RowIterator iter = inData.iterator();
		exec.setMessage("Searching first matching row...");
		try {
			int incount = 0;
			while (iter.hasNext()) {
				DataRow row = iter.next();
				RaTuple tuple = new RaTuple(row, _groupColNos);
				incount++;
				exec.setMessage("Reading row " + incount);
				DataCell cell = row.getCell(_argColNo);
				if (tupleHash.contains(tuple)) {
					tupleHash.get(tuple).accumulate(cell);
				} else {
					Accumulator accum = new Accumulator(_function, cell);
					tupleHash.put(tuple, accum);
				}
			}
			tupleHash.forEach((t,a) -> {
				//outcount++;
				//exec.setMessage("Writing row " + outcount);
				ArrayList<DataCell> cells = new ArrayList<>(Arrays.asList(t.getCells()));
				cells.add(a.getResult());
				container.addRowToTable(new DefaultRow("", cells));
			} );
		} catch (RowFilterIterator.RuntimeCanceledExecutionException rce) {
			throw rce.getCause();
		} finally {
			container.close();
		}
		return container.getTable();
	}

}

/*******************************************************************************
 * Aggregation type as enumeration plus extras
 */
enum AggType {
	NUL(null),
	BOOL(BooleanCell.TYPE),
	INT(IntCell.TYPE),
	REAL(DoubleCell.TYPE),
	//DATE(DateAndTimeCell.TYPE),
	CHAR(StringCell.TYPE);
	
	DataType _dataType;
	DataType getDataType() { return _dataType; }
	
	AggType(DataType type) {
		_dataType = type;
	}
	
	static AggType getAggType(DataType arg) {
		for (AggType atype : AggType.values()) {
			if (atype.getDataType().equals(arg))
				return atype;
		}
		return null;
	}
}

/*******************************************************************************
 * Aggregation function as enumeration plus extras
 */
enum AggFunctions {
	NUL(null),
	COUNT("Count"),
	SUM("Sum"),
	AVG("Average"),
	MAX("Max"),
	MIN("Min");
	
	private String _name;
	
	String getName() { return _name; }
	
	AggType getReturnType(AggType argtype) {
		return this.equals(COUNT) ? AggType.INT :
			this.equals(MAX) || this.equals(MIN) ? argtype :
			this.equals(AVG) && argtype.equals(AggType.INT) ? AggType.REAL :
			argtype.equals(AggType.INT) || argtype.equals(AggType.REAL) ? argtype :
			null;
	}

	AggFunctions(String name) {
		_name = name;
	}
}

/*******************************************************************************
 *
 * Manage computations on a single accumulator
 * 
 */

class Accumulator {
	AggFunctions _function;
	AggType _argtype;
	AggType _aggtype;
	Object _accumulator;
	
	public Accumulator(AggFunctions function, DataCell initValue) {
		_function = function;
		_argtype = AggType.getAggType(initValue.getType());
		_aggtype = _function.getReturnType(_argtype);
		_accumulator = toObject(initValue, _aggtype);
	}
	
	void accumulate(DataCell cell) {
		_accumulator = accumulate(_accumulator, toObject(cell, _argtype));
	}
	
	DataCell getResult() {
		return toDataCell(_accumulator, _aggtype);
	}

	// convert data cell to accumulator
	private Object toObject(DataCell dataCell, AggType type) {
		switch(type) {
		case BOOL: return ((BooleanCell)dataCell).getBooleanValue();
		case INT: return ((IntCell)dataCell).getIntValue();
		case CHAR: return ((StringCell)dataCell).getStringValue();
		case REAL: return ((DoubleCell)dataCell).getDoubleValue();
		//case DATE: return ((DateAndTimeCell)dataCell).getDateValue();
		default: return null;
		}
	}

	// convert accumulator to data cell
	private DataCell toDataCell(Object accumulator, AggType type) {
		switch (type) {
		case BOOL: return (boolean)accumulator ? BooleanCell.TRUE :BooleanCell.FALSE; 
		case INT: return new IntCell((Integer)accumulator);
		case REAL: return new DoubleCell((double)accumulator);
		case CHAR: return new StringCell((String)accumulator);
		//case DATE: TODO
		default: return null;
		}
	}

	// compute new value for accumulator
	private Object accumulate(Object accum, Object value) {
		switch (_function) {
		case COUNT: return (Integer)accum + 1; 
		case SUM: 
			switch (_aggtype) {
			case INT: return (Integer)accum + (Integer)value;
			case REAL: return (double)accum + (double)value;
			default: break;
			}
		case MAX: 
			switch (_aggtype) {
			case INT: return (Integer)accum < (Integer)value ? accum : value; 
			case REAL: return (double)accum < (double)value ? accum : value; 
			case CHAR: return ((String)accum).compareTo((String)value) < 0 ? accum : value;
			//case DATE: TODO
			default: break;
			}
		case MIN: 
			switch (_aggtype) {
			case INT: return (Integer)accum > (Integer)value ? accum : value; 
			case REAL: return (double)accum > (double)value ? accum : value; 
			case CHAR: return ((String)accum).compareTo((String)value) > 0 ? accum : value;
			//case DATE: TODO
			default: break;
			}
		default: break;
		}
		return null;
	}
}

