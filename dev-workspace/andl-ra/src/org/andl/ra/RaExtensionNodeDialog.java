package org.andl.ra;

import static org.knime.core.node.util.CheckUtils.checkSetting;

import java.awt.BorderLayout;

import javax.swing.JComboBox;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.util.DataTypeListCellRenderer;

/**
 * <code>NodeDialog</code> for the "RaExtension" node.
 * 
 * @author andl
 */
//public class RaExtensionNodeDialog extends DefaultNodeSettingsPane {
//
//    /**
//     * New pane for configuring the RaExtension node.
//     */
//    protected RaExtensionNodeDialog() {
//		addDialogComponent(new DialogComponentString(RaExtensionNodeModel.createSettingsColumnName(), "New column name"));
//		addDialogComponent(new DialogComponentStringSelection(RaExtensionNodeModel.createSettingsColumnType(), "Column type"));
//		addDialogComponent(new DialogComponentString(RaExtensionNodeModel.createSettingsExpression(), "Expression"));
//    }
//}

final class RaExtensionNodeDialog extends NodeDialogPane {

    private static final int DEFAULT_TEXT_SIZE = 25;
	private static final int HORIZONTAL_VERTICAL_GAB = 5;

    private final JTextField m_columnName;
    private final JComboBox<DataType> m_fieldType;
    private final JTextField m_value;

    private DataTableSpec m_dataTableSpec;

    /** Create new dialog. */
    @SuppressWarnings("unchecked")
    RaExtensionNodeDialog() {
        m_columnName = new JTextField(DEFAULT_TEXT_SIZE);
        m_value = new JTextField(DEFAULT_TEXT_SIZE);

        m_fieldType = new JComboBox<DataType>();
        m_fieldType.setRenderer(new DataTypeListCellRenderer());

        for (TypeCellFactory factory : TypeCellFactory.values()) {
            m_fieldType.addItem(factory.getDataType());
        }
        m_fieldType.setSelectedIndex(0);

        final JPanel northValuePanel = new JPanel(new BorderLayout(5, 5));
        northValuePanel.add(m_columnName, BorderLayout.NORTH);
        northValuePanel.add(m_fieldType, BorderLayout.NORTH);
        northValuePanel.add(m_value, BorderLayout.NORTH);

        JPanel tabPanel = new JPanel(new BorderLayout());
        tabPanel.add(northValuePanel, BorderLayout.CENTER);

        addTab("Settings", tabPanel);
    }

    /** {@inheritDoc} */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final DataTableSpec[] specs)
        throws NotConfigurableException {
        RaExtensionConfig config = new RaExtensionConfig();
        config.loadInDialog(settings, specs[0]);
        m_dataTableSpec = specs[0];
        m_value.setText(config.getValue());
        m_fieldType.setSelectedItem(config.getCellFactory().getDataType());
        setText(m_columnName, config.getNewColumnName());
    }

    /** {@inheritDoc} */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        RaExtensionConfig config = new RaExtensionConfig();
        TypeCellFactory forDataType = TypeCellFactory.forDataType((DataType)m_fieldType.getSelectedItem());
        config.setValue(m_value.getText());

        config.setCellFactory(forDataType);
        config.setNewColumnName(getText(m_columnName, "New column name must not be empty."));
        config.save(settings);
    }

    @Override
    public void onClose() {
        m_dataTableSpec = null;
    }

    private static void setText(final JTextField appendColumnField, final String newColumnName) {
        appendColumnField.setEnabled(false);
        if (newColumnName != null) {
            appendColumnField.setText(newColumnName);
            appendColumnField.setEnabled(true);
        }
    }

    private static String getText(final JTextField field, final String messageIfNotExist)
        throws InvalidSettingsException {
        String text = field.getText();
        checkSetting(text != null, messageIfNotExist);
        return text;
    }

}

