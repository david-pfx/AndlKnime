package org.andl.ra.extension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlExpression;
import org.knime.core.data.BooleanValue;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.IntValue;
import org.knime.core.data.StringValue;
//import org.knime.core.data.date.DateAndTimeCell;
//import org.knime.core.data.date.DateAndTimeValue;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.InvalidSettingsException;

class RaExtensionJexl {
	private DataTableSpec _intablespec;
	private DataType _outcoltype;
	private JexlEngine _jexl = new JexlBuilder().cache(512).strict(true).silent(false).create();
	private RaContext _context;
	private ArrayList<DataColumnSpec> _colspecs = new ArrayList<>();
	private JexlExpression _expr;

	// create Jexl wrapper from specs and settings
	RaExtensionJexl(DataTableSpec intablespec, DataColumnSpec outcolspec, String expression) 
	throws InvalidSettingsException {
		_intablespec = intablespec;
		_outcoltype = outcolspec.getType();
	    HashMap<String,Integer> map = new HashMap<>();
	    for (DataColumnSpec colspec : _intablespec) {
	    	map.put(colspec.getName(), _colspecs.size());
	    	_colspecs.add(colspec);
	    }
	    _context = new RaContext(map);
	    try {
	    	_expr = _jexl.createExpression(expression);
		} catch (Exception e) {
			throw new InvalidSettingsException("The expression is not valid: " + e.getMessage(), e);
	    }
	    
	}

	// evaluate the expression on a row and return a cell value
	DataCell evaluate(DataRow row) {
		_context._currentrow = row;
	    return getCell(_expr.evaluate(_context));
	}
	
	// convert object value to cell value
    DataCell getCell(Object value) {
    	if (_outcoltype.equals(BooleanCell.TYPE)) return (boolean)value ? BooleanCell.TRUE : BooleanCell.FALSE;
    	if (_outcoltype.equals(IntCell.TYPE)) return IntCell.IntCellFactory.create((int)value);
    	if (_outcoltype.equals(DoubleCell.TYPE)) return DoubleCell.DoubleCellFactory.create((double)value);
    	if (_outcoltype.equals(StringCell.TYPE)) return StringCell.StringCellFactory.create((String)value);
    	//if (cell instanceof DateAndTimeValue) return DateAndTimeCell.UTILITY.create((int)value);
    	return null;
    }

}

// Dynamic context for use by expression evaluator
class RaContext implements JexlContext {
	final Map<String,Integer> _map;
	DataRow _currentrow;
	
    public RaContext(Map<String,Integer> map) {
    	_map = map;
    }

    @Override
    public boolean has(String name) {
        return _map.containsKey(name);
    }

    // get an object value from the row by index
    @Override
    public Object get(String name) {
        return getValue(_currentrow.getCell(_map.get(name)));
    }

    @Override
    public void set(String name, Object value) {
        //throw new InvalidDataException();
    }
    
    Object getValue(DataCell cell) {
    	if (cell instanceof BooleanValue) return ((BooleanValue)cell).getBooleanValue();
    	if (cell instanceof IntValue) return ((IntValue)cell).getIntValue();
    	if (cell instanceof DoubleValue) return ((DoubleValue)cell).getDoubleValue();
    	if (cell instanceof StringValue) return ((StringValue)cell).getStringValue();
    	//if (cell instanceof DateAndTimeValue) return ((DateAndTimeValue)cell).getUTCTimeInMillis(); //BUG
    	return null;
    }
}
	