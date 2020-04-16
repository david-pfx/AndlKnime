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

package org.andl.ra;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;

/*******************************************************************************
 * Internal data types as enumeration plus extras
 */
public enum RaType {
	//NUL(null),
	BOOL(BooleanCell.TYPE) {
	    @Override
	    public DataCell getDataCell(Object arg) {
	        return (Boolean)arg ? BooleanCell.TRUE : BooleanCell.FALSE; 
	    }
	},
	INT(IntCell.TYPE) {
		public DataCell getDataCell(Object arg) {
			return new IntCell((Integer)arg);
		}
	},
	LONG(IntCell.TYPE) {
		public DataCell getDataCell(Object arg) {
			return new LongCell((Long)arg);
		}
	},
	DOUBLE(DoubleCell.TYPE) {
	    public DataCell getDataCell(Object arg) {
	        return new DoubleCell((Double)arg);
	    }
	//DATE(DateAndTimeCell.TYPE),
	},
	STRING(StringCell.TYPE) {
	    public DataCell getDataCell(Object arg) {
	        return new IntCell((Integer)arg);
	    }
	};
	
	DataType _dataType;
	public DataType getDataType() { return _dataType; }
	
	RaType(DataType type) {
		_dataType = type;
	}
	
    abstract DataCell getDataCell(Object arg) throws Exception;
	
	// compute aggregation type
	public static RaType getRaType(DataType arg) {
		if (arg == null) return null;
		for (RaType atype : RaType.values()) {
			if (arg.equals(atype.getDataType()))
				return atype;
		}
		return null;
	}
}
