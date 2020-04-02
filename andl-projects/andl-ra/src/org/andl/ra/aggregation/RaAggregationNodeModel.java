package org.andl.ra.aggregation;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.andl.ra.RaTuple;
import org.knime.base.node.preproc.filter.row.RowFilterIterator;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowIterator;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
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

	private final SettingsModelFilterString _columnFilterSettings = createColumnFilterSettings();
	private final SettingsModelString _oldColumnNameSettings = createSettingsOldColumnName();
	private final SettingsModelString _newColumnNameSettings = createSettingsNewColumnName();
	private final SettingsModelString _aggFunctionSettings = createSettingsAggFunction();

	private AggManager _aggManager;

	static SettingsModelFilterString createColumnFilterSettings() {
		return new SettingsModelFilterString(KEY_COLUMN_SELECTOR);
	}

	static SettingsModelString createSettingsOldColumnName() {
		return new SettingsModelString(KEY_OLD_COLUMN_NAME, "");
	}

	static SettingsModelString createSettingsNewColumnName() {
		return new SettingsModelString(KEY_NEW_COLUMN_NAME, "new-total");
	}

	static SettingsModelString createSettingsAggFunction() {
		return new SettingsModelString(KEY_AGG_FUNCTION, "Sum");
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
                _columnFilterSettings.getExcludeList().toArray(new String[0]),
                _oldColumnNameSettings.getStringValue(),
                _newColumnNameSettings.getStringValue(),
                _aggFunctionSettings.getStringValue());
        return new DataTableSpec[] { 
       		_aggManager.getOutputSpec()
        };
    }

    /** {@inheritDoc} */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        LOGGER.debug("*save to settings=" + settings);
        
        // might not be valid but who cares
        _columnFilterSettings.saveSettingsTo(settings);
    	_oldColumnNameSettings.saveSettingsTo(settings);
    	_newColumnNameSettings.saveSettingsTo(settings);
    	_aggFunctionSettings.saveSettingsTo(settings);
    }

    /** {@inheritDoc} */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        LOGGER.debug("*load from settings=" + settings);

        // muad be valid because it's been validated
        _columnFilterSettings.loadSettingsFrom(settings);
    	_oldColumnNameSettings.loadSettingsFrom(settings);
    	_newColumnNameSettings.loadSettingsFrom(settings);
    	_aggFunctionSettings.loadSettingsFrom(settings);
    }

    /** {@inheritDoc} */
    @Override
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        LOGGER.debug("*validate settings=" + settings);
        
        _columnFilterSettings.validateSettings(settings);
    	_oldColumnNameSettings.validateSettings(settings);
    	_newColumnNameSettings.validateSettings(settings);
    	_aggFunctionSettings.validateSettings(settings);
    }
    
    //==========================================================================
    // implementation
    //
    
    // Carry out tests to see if settings are valid
    Pair<Boolean, String> checkSettings() {
        List<String> groupCols =_columnFilterSettings.getExcludeList(); 
        List<String> availCols =_columnFilterSettings.getIncludeList(); 
        if (availCols.isEmpty())
        	return Pair.create(false, "no columns available for aggregation");
        
		String aggCol = _oldColumnNameSettings.getStringValue();
		if (!availCols.contains(aggCol))
			return Pair.create(false, "invalid column for aggregation: " + aggCol);

    	String aggFunc = _aggFunctionSettings.getStringValue();
    	List<String> availFuncs = AggFunction.getAllNames(); //TODO: filter by type
		if (!availFuncs.contains(aggFunc))
			return Pair.create(false, "not a valid aggregating function: " + aggFunc);

    	String aggedCol = _newColumnNameSettings.getStringValue();
    	if (" ".compareTo(aggedCol) >= 0 || groupCols.contains(aggedCol))
    		return Pair.create(false, "not a valid aggregated column name: " + aggedCol);
    	// ok
    	return Pair.create(true, null);
    }
    
}

//==============================================================================
//
// Implementation
//

/*******************************************************************************
 *  
 * Functions and data to manage aggregation
 */
class AggManager {
	DataTableSpec _inputSpec, _outputSpec;
	int[] _groupColNos;
	int _argColNo;
	AggFunction _function = null;

	private static final NodeLogger LOGGER = NodeLogger.getLogger(AggManager.class);
	DataTableSpec getOutputSpec() { return _outputSpec; }

	AggManager(DataTableSpec inputSpec, String[] groupCols, String argCol, String retCol, String aggFunction) 
	throws InvalidSettingsException {
        LOGGER.debug(String.format("*ctor spec=%s gcols=%s acol=%s rcol=%s afunc=%s",
        		inputSpec, Arrays.asList(groupCols), argCol, retCol, aggFunction));
		_inputSpec = inputSpec;
		_groupColNos = inputSpec.columnsToIndices(groupCols);
		_argColNo = inputSpec.findColumnIndex(argCol);
		LOGGER.assertLog(_argColNo >= 0, "Aggregation column not valid");
		
		for (AggFunction value : AggFunction.values()) {
			if (value.getName().equals(aggFunction)) {
				_function = value;
				break;
			}
		}
		LOGGER.assertLog(_function != null, "Unknown function");
		
		AggType argType = AggType.getAggType(_inputSpec.getColumnSpec(_argColNo).getType());
		LOGGER.assertLog(argType != null, "Unsupported column type: " + aggFunction);
		
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
		HashMap<RaTuple, Accumulator> tupleHash = new HashMap<>();
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
				if (tupleHash.containsKey(tuple)) {
					tupleHash.get(tuple).accumulate(cell);
				} else {
					Accumulator accum = new Accumulator(_function, cell);
					tupleHash.put(tuple, accum);
				}
			}
			int outcount = 0;
			for (Entry<RaTuple, Accumulator> entry : tupleHash.entrySet()) {
				outcount++;
				exec.setMessage("Writing row " + outcount);
				ArrayList<DataCell> cells = new ArrayList<>(Arrays.asList(entry.getKey().getCells()));
				cells.add(entry.getValue().getResult());
				container.addRowToTable(new DefaultRow("Row" + outcount, cells));
			}
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
	//NUL(null),
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
	
	// compute aggregation type
	static AggType getAggType(DataType arg) {
		if (arg == null) return null;
		for (AggType atype : AggType.values()) {
			if (arg.equals(atype.getDataType()))
				return atype;
		}
		return null;
	}
}

/*******************************************************************************
 * Aggregation function as enumeration plus extras
 */
enum AggFunction {
	//NUL(null),
	COUNT("Count"),
	SUM("Sum"),
	AVG("Average"),
	MAX("Max"),
	MIN("Min");
	
	private String _name;
	
	String getName() { return _name; }

	// get all names as a list
	static List<String> getAllNames() {
		return Stream.of(AggFunction.values())
				.map(v -> v.getName())
				.collect(Collectors.toList());
	}
	
	// compute function return type
	AggType getReturnType(AggType argtype) {
		return this.equals(COUNT) ? AggType.INT :
			this.equals(MAX) || this.equals(MIN) ? argtype :
			this.equals(AVG) && argtype.equals(AggType.INT) ? AggType.REAL :
			argtype.equals(AggType.INT) || argtype.equals(AggType.REAL) ? argtype :
			null;
	}

	AggFunction(String name) {
		_name = name;
	}
}

/*******************************************************************************
 *
 * Manage computations on a single accumulator
 * 
 */

class Accumulator {
	AggFunction _function;
	AggType _argtype;
	AggType _aggtype;
	Object _accumulator;
	
	public Accumulator(AggFunction function, DataCell initValue) {
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

