/**
 * Implement data tuple with equals by value and hashCode. 
 *
 * @author Andl
 */

package org.andl.ra.set;

import java.util.Arrays;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;

public class RaTuple {
    private DataCell[] _cells;
    
    public DataCell[] getCells() {
		return _cells;
	}

    /**
     * Creates a new RaTuple from an array of cells.
     * @param cells The cells comprising the tuple.
     */
    public RaTuple(final DataCell[] cells) {
    	_cells = cells;
    }

    /**
     * Creates a new RaTuple from a DataRow
     * @param row The DataRow
     */
    public RaTuple(final DataRow row) {
    	_cells = new DataCell[row.getNumCells()];
    	for (int i = 0; i < _cells.length; ++i)
    		_cells[i] = row.getCell(i);
    }

    /**
     * Creates a new RaTuple from a DataRow and indexes
     * @param row The DataRow
     * @param indexes A list of indexes into the row
     */
    public RaTuple(final DataRow row, int[] indexes) {
    	_cells = new DataCell[indexes.length];
    	for (int i = 0; i < indexes.length; ++i)
    		_cells[i] = row.getCell(indexes[i]);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Arrays.hashCode(_cells);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof RaTuple)) return false;
        
        RaTuple that = (RaTuple)obj;
        for (int i = 0; i < this._cells.length; i++) {
            DataCell thisCell = this._cells[i];
            DataCell thatCell = that._cells[i];
            if (!thisCell.equals(thatCell)) return false;
        }
        return true;
    }
}

