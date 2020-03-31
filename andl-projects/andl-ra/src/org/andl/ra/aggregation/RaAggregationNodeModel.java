package org.andl.ra.aggregation;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Dictionary;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;

import org.andl.ra.RaTuple;
import org.knime.base.node.io.filereader.DataCellFactory;
import org.knime.base.node.preproc.filter.row.RowFilterIterator;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowIterator;
import org.knime.core.data.container.ColumnRearranger;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.BooleanCell;
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

	BufferedDataTable execute(BufferedDataTable bufferedDataTable, ExecutionContext exec) {
		// TODO Auto-generated method stub
		return null;
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
//	DataType getReturnType(DataType argtype) {
//		return this.equals(COUNT) ? IntCell.TYPE :
//			this.equals(MAX) || this.equals(MIN) ? argtype :
//			this.equals(AVG) && argtype.equals(IntCell.TYPE) ? DoubleCell.TYPE :
//			argtype.equals(IntCell.TYPE) || argtype.equals(DoubleCell.TYPE) ? argtype :
//			null;
//	}

	AggFunctions(String name) {
		_name = name;
	}
}

//enum Functions {
//	NUL,
//	COUNT, 
//	ISUM, IAVG, IMAX, IMIN,
//	FSUM, FAVG, FMAX, FMIN,
//	
//}

///*******************************************************************************
//* Internal class to compute various column specs and maps.
//* <br>
//* All pre-calculated because they get used on every row
//*/
//class SpecGenerator {
//	DataTableSpec _inputSpec, _outputSpec;
//	int[] _keepColMap, _aggregateColMap;
//
//	SpecGenerator(DataTableSpec inputSpec, List<String> aggColNames) 
//	throws InvalidSettingsException {
//		_inputSpec = inputSpec;
////		_outputSpec = replaceCols
////		_leftInputSpec = leftInputSpec;
////		_rightInputSpec = rightInputSpec;
////
////		_joinSpec = getJoinSpec(_leftInputSpec, _rightInputSpec);
////		_leftSpec = specMinus(_leftInputSpec, _joinSpec);
////		_rightSpec = specMinus(_rightInputSpec, _joinSpec);
////		
////		_leftcolmap = getColMap(_leftSpec, _leftInputSpec);
////		_joinleftcolmap = getColMap(_joinSpec, _leftInputSpec);
////		_rightcolmap = getColMap(_rightSpec, _rightInputSpec);
////		_joinrightcolmap = getColMap(_joinSpec, _rightInputSpec);
//
//	}
//	
//	// get a column map for getting required columns from a row
//	private int[] getColMap(DataTableSpec destSpec, DataTableSpec sourceSpec) {
//		return destSpec.stream()
//			.mapToInt(s -> sourceSpec.findColumnIndex(s.getName()))
//			.toArray();
//	}
//
//	// return a spec for the left minus right columns 
//	private DataTableSpec specMinus(DataTableSpec leftSpec, DataTableSpec rightSpec) {
//		DataColumnSpec[] cols = leftSpec.stream()
//				.filter(s -> !rightSpec.containsName(s.getName()))
//				.toArray(DataColumnSpec[]::new);
//			return new DataTableSpec(cols);
//	}
//
//	// return a spec for the join columns
//	private DataTableSpec getJoinSpec(DataTableSpec leftSpec, DataTableSpec rightSpec) 
//	throws InvalidSettingsException {
//		DataColumnSpec[] jcols = leftSpec.stream()
//			.filter(s -> rightSpec.containsName(s.getName()))
//			.toArray(DataColumnSpec[]::new);
//		for (DataColumnSpec jcol : jcols) {
//			DataColumnSpec rcol = rightSpec.getColumnSpec(jcol.getName());
//			if (rcol != null && rcol.getType() != jcol.getType())
//				throw new InvalidSettingsException("Join columns not same type: " + jcol.getName());
//		}
//		return new DataTableSpec(jcols);
//	}
//}

/*******************************************************************************
 *
 * Internal class to manage type conversions and accumulators
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


//class Accumulators {
//	Functions[] _functions;
//	Object[] _accumulators;
//	
//	public Accumulators(Functions[] functions, DataCell[] initValues) {
//		_functions = functions;
//		_accumulators = new Object[_functions.length];
//		for (int i = 0; i < _accumulators.length; i++)
//			_accumulators[i] = toObject(initValues[i], _functions[i]);
//	}
//	
//	void accumulate(DataCell[] cells) {
//		for (int i = 0; i < cells.length; i++)
//			_accumulators[i] = accumulate(_accumulators[i], _functions[i], toObject(cells[i], _functions[i]));
//	}
//	
//	DataCell[] getResults() {
//		DataCell[] cells = new DataCell[_accumulators.length];
//		for (int i = 0; i < cells.length; i++)
//			cells[i] = toDataCell(_accumulators[i], _functions[i]);
//		return cells;
//	}
//
//	// convert accumulator to data cell
//	private DataCell toDataCell(Object accumulator, Functions function) {
//		switch (function) {
//		case NUL: return null;
//		case COUNT:  
//		case ISUM: 
//		case IMAX: 
//		case IMIN: return new IntCell((Integer)accumulator);
//		case FSUM: 
//		case FMAX: 
//		case FMIN: return new DoubleCell((double)accumulator);
//		default: return null;
//		}
//	}
//
//	// convert data cell to accumulator
//	private Object toObject(DataCell dataCell, Functions function) {
//		switch (function) {
//		case NUL: return null;
//		case COUNT: return 0;
//		case ISUM: 
//		case IMAX: 
//		case IMIN: return ((IntCell)dataCell).getIntValue();
//		case FSUM: 
//		case FMAX: 
//		case FMIN: return ((DoubleCell)dataCell).getDoubleValue();
//		default: return null;
//		}
//	}
//
//	// compute new value for accumulator
//	private Object accumulate(Object accumulator, Functions function, Object dataValue) {
//		switch (function) {
//		case NUL: return accumulator;
//		case COUNT: return (Integer)accumulator + 1; 
//		case ISUM: return (Integer)accumulator + (Integer)dataValue;
//		case IMAX: {
//			Integer a = (Integer)accumulator;
//			Integer d = (Integer)dataValue; 
//			return a < d ? d : a; 
//		}
//		case IMIN: {
//			Integer a = (Integer)accumulator;
//			Integer d = (Integer)dataValue; 
//			return a > d ? d : a; 
//		}
//		case FSUM: return (double)accumulator + (double)dataValue; 
//		case FMAX: {
//			double a = (double)accumulator;
//			double d = (double)dataValue; 
//			return a < d ? d : a; 
//		} 
//		case FMIN: {
//			double a = (double)accumulator;
//			double d = (double)dataValue; 
//			return a > d ? d : a; 
//		} 
//		default: return null;
//		}
//	}
}

