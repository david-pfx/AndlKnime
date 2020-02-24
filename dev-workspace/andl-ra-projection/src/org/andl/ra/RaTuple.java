/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ------------------------------------------------------------------------
 *
 * History
 *   25.11.2009 (Heiko Hofer): created
 */
package org.andl.ra;

import java.util.Arrays;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;

/**
 * Implement data tuple with equals by value and hashCode. 
 *
 * @author Andl
 */
public class RaTuple {
    private DataCell[] m_cells;

    /**
     * Creates a new RaTuple from an array of cells.
     * @param cells The cells comprising the tuple.
     */
    public RaTuple(final DataCell[] cells) {
    	m_cells = cells;
//        m_cells = new ArrayList<DataCell>();
//        for (DataCell cell : cells) { 
//        	m_cells.add(cell); 
//        };
    }

    /**
     * Creates a new RaTuple from a DataRow
     * @param row The DataRow
     */
    public RaTuple(final DataRow row) {
    	m_cells = new DataCell[row.getNumCells()];
    	for (int i = 0; i < m_cells.length; ++i)
    		m_cells[i] = row.getCell(i);
//        m_cells = new ArrayList<DataCell>();
//        row.forEach(cell -> { m_cells.add(cell); });
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Arrays.hashCode(m_cells);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof RaTuple)) return false;
        
        RaTuple that = (RaTuple)obj;
        for (int i = 0; i < this.m_cells.length; i++) {
            DataCell thisCell = this.m_cells[i];
            DataCell thatCell = that.m_cells[i];
            if (!thisCell.equals(thatCell)) return false;
        }
        return true;
    }
}

