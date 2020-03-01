package org.andl.ra;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

final class RaExtensionConfig {
    private static final String NEW_COLUMN_NAME = "new-column-name";
    private static final String TYPE = "column-type";
    static final String VALUE = "column-value";

    private String m_newColumnName;
    private String m_value;
    private TypeCellFactory m_cellFactory;

    /**
     * @return the newColumnName
     */
    public String getNewColumnName() {
        return m_newColumnName;
    }

    /**
     * @param newColumnName the newColumnName to set
     */
    public void setNewColumnName(final String newColumnName) {
        m_newColumnName = newColumnName;
    }

    /**
     * @return the value
     */
    public String getValue() {
        return m_value;
    }

    /**
     * @param value the value to set
     */
    public void setValue(final String value) {
        m_value = value;
    }

    /**
     * @return the cellFactory
     */
    public TypeCellFactory getCellFactory() {
        return m_cellFactory;
    }

    /**
     * @param cellFactory the cellFactory to set
     */
    public void setCellFactory(final TypeCellFactory cellFactory) {
        m_cellFactory = cellFactory;
    }

    /**
     * Save current configuration.
     *
     * @param settings To save to.
     */
    void save(final NodeSettingsWO settings) {
        settings.addString(NEW_COLUMN_NAME, m_newColumnName);
        settings.addString(VALUE, m_value);
        settings.addString(TYPE, m_cellFactory.toString());
    }

    /**
     * Load config in node model.
     *
     * @param settings To load from.
     * @throws InvalidSettingsException If invalid.
     */
    void loadInModel(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_newColumnName = settings.getString(NEW_COLUMN_NAME);
        m_value = settings.getString(VALUE);
        m_cellFactory = getEnum(settings.getString(TYPE));
    }

    /**
     * Load config in dialog.
     *
     * @param settings To load from
     * @param in Current input spec
     */
    void loadInDialog(final NodeSettingsRO settings, final DataTableSpec in) {
        m_newColumnName = settings.getString(NEW_COLUMN_NAME, null);
        m_value = settings.getString(VALUE, null);
        try {
            m_cellFactory = getEnum(settings.getString(TYPE, TypeCellFactory.STRING.toString()));
        } catch (InvalidSettingsException e) {
            m_cellFactory = TypeCellFactory.STRING;
        }
    }

    /**
     * @param string
     * @param b
     * @return
     */
    private TypeCellFactory getEnum(final String string) throws InvalidSettingsException {
        try {
            return TypeCellFactory.valueOf(string);
        } catch (IllegalArgumentException e) {
            // NOOP
        }
        throw new InvalidSettingsException("invalid type: " + string);
    }
}
