package com.cs324.client;

import javax.swing.*;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.DefaultTableCellRenderer;
import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import static com.cs324.client.ClientStyle.*;

/**
 * Swing client for preparing MAX, PRIMESUM and PRIMECOUNT input.
 * Flow: enter or load data -> validate it -> add a local preview row.
 * This class does not yet contact a coordinator or calculate job results.
 */

public class ClientApp extends JFrame {

    // Colours, rounded panels and icons are defined in ClientStyle.java.

    // Keep controls as fields so button handlers can read input and update messages.
    private final JComboBox<String> jobTypeBox =
            new JComboBox<>(new String[]{"MAX", "PRIMESUM", "PRIMECOUNT"});

    private final InputArea inputArea = new InputArea();
    private final JLabel exampleLabel = new JLabel();
    private final JLabel statusLabel =
            new JLabel("Backend not connected — input preview only");

    // The model stores preview rows; JTable displays them. Users cannot edit the rows.
    private final DefaultTableModel jobModel = new DefaultTableModel(
            new String[]{"Local reference", "Operation", "Input", "Status", "Result"},
            0
    ) {
        @Override
        public boolean isCellEditable(int row, int column) {
            return false;
        }
    };

    // References belong to this window only; they are not backend job IDs.
    private int nextReference = 1;
    private final JLabel previewCount = new JLabel("0 previews");
    // CardLayout displays either the empty-state message or the populated table.
    private final CardLayout historyLayout = new CardLayout();
    private final JPanel historyContent = new JPanel(historyLayout);

    /** Builds the window, arranges its sections and connects controls to handlers. */
    public ClientApp() {
        super("CS324 Distributed Job Client");

        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setMinimumSize(new Dimension(900, 780));

        // Outer layout: heading at the top, workspace in the centre, status at the bottom.
        JPanel root = new JPanel(new BorderLayout(0, 20));
        root.setBackground(BACKGROUND);
        root.setBorder(BorderFactory.createEmptyBorder(24, 28, 16, 28));
        setContentPane(root);

        // The heading includes a breadcrumb, preview badge and decorative server graphic.
        JPanel header = ClientStyle.header();
        header.setBorder(BorderFactory.createEmptyBorder(0, 20, 6, 20));
        JPanel heading = transparentPanel(null);
        heading.setLayout(new BoxLayout(heading, BoxLayout.Y_AXIS));
        JLabel breadcrumb = label("CS324   /   Distributed computing", 13, Font.PLAIN, MUTED);
        breadcrumb.setIcon(icon("home", 18, MUTED));
        breadcrumb.setIconTextGap(10);
        heading.add(breadcrumb);
        heading.add(Box.createVerticalStrut(14));
        heading.add(label("Job workspace", 30, Font.BOLD, INK));
        heading.add(Box.createVerticalStrut(5));
        heading.add(label("Prepare your data and review computational jobs.", 16, Font.PLAIN, MUTED));
        header.add(heading, BorderLayout.CENTER);
        JButton mode = button("Preview mode", "eye", false);
        mode.setFont(BODY.deriveFont(12f));
        mode.setBorder(BorderFactory.createEmptyBorder(7, 12, 7, 12));
        mode.setFocusable(false);
        mode.setCursor(Cursor.getDefaultCursor());
        mode.setToolTipText("Input previews are available. Backend submission is not connected.");
        JPanel modePanel = transparentPanel(new FlowLayout(FlowLayout.RIGHT, 0, 0));
        modePanel.add(mode);
        header.add(modePanel, BorderLayout.EAST);
        root.add(header, BorderLayout.NORTH);

        // Rounded cards group the form and preview history without changing their behaviour.
        JPanel workspace = new Workspace();
        JPanel composer = card(new BorderLayout(0, 20));
        composer.add(sectionHeading("New job", "Choose an operation and provide input data.", "document"),
                BorderLayout.NORTH);

        // Operation selection determines the example text and validation rules.
        JPanel fields = transparentPanel(new BorderLayout(0, 16));
        JPanel selectorPanel = transparentPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
        JLabel operationLabel = label("Operation   ", 14, Font.BOLD, INK);
        operationLabel.setLabelFor(jobTypeBox);
        selectorPanel.add(operationLabel);
        jobTypeBox.setFont(BODY);
        jobTypeBox.setBackground(Color.WHITE);
        jobTypeBox.setForeground(INK);
        jobTypeBox.setBorder(new RoundBorder(false));
        // A rounded dropdown keeps the same keyboard and selection behaviour as Swing.
        jobTypeBox.setUI(new javax.swing.plaf.basic.BasicComboBoxUI() {
            @Override
            protected JButton createArrowButton() {
                JButton arrow = new javax.swing.plaf.basic.BasicArrowButton(
                        SwingConstants.SOUTH, Color.WHITE, Color.WHITE, MUTED, Color.WHITE);
                arrow.setBorder(BorderFactory.createEmptyBorder(0, 8, 0, 8));
                arrow.setFocusable(false);
                return arrow;
            }
        });
        jobTypeBox.setRenderer(new DefaultListCellRenderer() {
            @Override
            public Component getListCellRendererComponent(JList<?> list, Object value,
                    int index, boolean selected, boolean focused) {
                super.getListCellRendererComponent(list, value, index, selected, focused);
                setBackground(selected ? LIGHT_BLUE : Color.WHITE);
                setForeground(INK);
                setBorder(BorderFactory.createEmptyBorder(5, 8, 5, 8));
                return this;
            }
        });
        jobTypeBox.setPreferredSize(new Dimension(240, 44));
        selectorPanel.add(jobTypeBox);
        JPanel operationRow = transparentPanel(new BorderLayout());
        operationRow.add(selectorPanel, BorderLayout.WEST);
        JLabel sourceHint = label("Manual entry or CSV", 13, Font.PLAIN, MUTED);
        sourceHint.setIcon(icon("keyboard", 20, MUTED));
        sourceHint.setIconTextGap(9);
        operationRow.add(sourceHint, BorderLayout.EAST);
        fields.add(operationRow, BorderLayout.NORTH);

        // One scrollable input area is shared by typed data and imported CSV contents.
        inputArea.setLineWrap(true);
        inputArea.setWrapStyleWord(true);
        inputArea.setFont(BODY);
        inputArea.setForeground(INK);
        inputArea.setBackground(Color.WHITE);
        inputArea.setCaretColor(BLUE);
        inputArea.setSelectionColor(BLUE);
        inputArea.setSelectedTextColor(Color.WHITE);
        inputArea.setMargin(new Insets(5, 5, 5, 5));
        inputArea.getAccessibleContext().setAccessibleName("Job input data");

        JPanel inputPanel = transparentPanel(new BorderLayout(0, 8));
        exampleLabel.setFont(BODY.deriveFont(13f));
        exampleLabel.setForeground(MUTED);
        exampleLabel.setLabelFor(inputArea);
        inputPanel.add(exampleLabel, BorderLayout.NORTH);
        JScrollPane inputScroll = new JScrollPane(inputArea);
        inputScroll.setBorder(new RoundBorder(false));
        inputScroll.setBackground(Color.WHITE);
        inputScroll.getViewport().setBackground(Color.WHITE);
        inputScroll.setPreferredSize(new Dimension(700, 100));
        inputPanel.add(inputScroll, BorderLayout.CENTER);
        inputPanel.add(label("CSV: integers separated by commas; no headers or quotes. Up to 1 MB.",
                12, Font.PLAIN, MUTED), BorderLayout.SOUTH);
        fields.add(inputPanel, BorderLayout.CENTER);
        composer.add(fields, BorderLayout.CENTER);

        // Secondary actions stay on the left; validation is the primary action on the right.
        // Mnemonics provide keyboard shortcuts, such as Alt+L for loading a file on Windows.
        JButton loadCsvButton = button("Load CSV", "folder", false);
        JButton previewButton = button("Validate & preview", "play", true);
        JButton clearButton = button("Clear input", "trash", false);
        loadCsvButton.setMnemonic('L');
        previewButton.setMnemonic('V');
        clearButton.setMnemonic('C');
        previewButton.setToolTipText("Validate input and add a preview. No job is submitted.");
        JPanel actions = transparentPanel(new BorderLayout());
        JPanel secondaryActions = transparentPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
        secondaryActions.add(loadCsvButton);
        secondaryActions.add(Box.createHorizontalStrut(10));
        secondaryActions.add(clearButton);
        actions.add(secondaryActions, BorderLayout.WEST);
        actions.add(previewButton, BorderLayout.EAST);
        composer.add(actions, BorderLayout.SOUTH);
        workspace.add(composer, BorderLayout.NORTH);

        // The second card switches between an empty-state message and real preview rows.
        JPanel history = card(new BorderLayout(0, 14));
        JPanel historyHeading = transparentPanel(new BorderLayout());
        historyHeading.add(sectionHeading("Job previews",
                "Review and verify your input before submitting jobs.", "list"), BorderLayout.CENTER);
        previewCount.setFont(BODY.deriveFont(12f));
        previewCount.setForeground(MUTED);
        historyHeading.add(previewCount, BorderLayout.EAST);
        history.add(historyHeading, BorderLayout.NORTH);

        // Sorting changes the displayed order, while the model remains the source of row data.
        JTable jobTable = new JTable(jobModel);
        jobTable.setFillsViewportHeight(true);
        jobTable.setAutoCreateRowSorter(true);
        jobTable.setFont(BODY.deriveFont(13f));
        jobTable.setRowHeight(40);
        jobTable.setShowVerticalLines(false);
        jobTable.setGridColor(LINE);
        jobTable.setIntercellSpacing(new Dimension(0, 1));
        jobTable.setSelectionBackground(LIGHT_BLUE);
        jobTable.setSelectionForeground(INK);
        jobTable.getTableHeader().setReorderingAllowed(false);
        jobTable.getTableHeader().setPreferredSize(new Dimension(0, 38));

        // Use white rows and subtle horizontal rules, preserving selection contrast.
        jobTable.setDefaultRenderer(Object.class, new DefaultTableCellRenderer() {
            @Override
            public Component getTableCellRendererComponent(JTable table, Object value,
                    boolean selected, boolean focused, int row, int column) {
                super.getTableCellRendererComponent(table, value, selected, focused, row, column);
                setBorder(BorderFactory.createEmptyBorder(0, 10, 0, 10));
                if (!selected) {
                    setBackground(row % 2 == 0 ? Color.WHITE : new Color(248, 250, 255));
                    setForeground(INK);
                }
                setToolTipText(value == null ? null : value.toString());
                return this;
            }
        });
        // Plain headers retain an arrow showing the active sort direction.
        jobTable.getTableHeader().setDefaultRenderer(new DefaultTableCellRenderer() {
            @Override
            public Component getTableCellRendererComponent(JTable table, Object value,
                    boolean selected, boolean focused, int row, int column) {
                super.getTableCellRendererComponent(table, value, selected, focused, row, column);
                setBackground(new Color(245, 248, 255));
                setForeground(MUTED);
                setFont(BODY.deriveFont(Font.BOLD, 12f));
                setBorder(BorderFactory.createCompoundBorder(
                        BorderFactory.createMatteBorder(0, 0, 1, 0, LINE),
                        BorderFactory.createEmptyBorder(0, 10, 0, 10)));
                for (RowSorter.SortKey key : table.getRowSorter().getSortKeys()) {
                    if (key.getColumn() == table.convertColumnIndexToModel(column)) {
                        if (key.getSortOrder() == SortOrder.ASCENDING) setText(value + " \u2191");
                        if (key.getSortOrder() == SortOrder.DESCENDING) setText(value + " \u2193");
                        break;
                    }
                }
                return this;
            }
        });
        jobTable.getTableHeader().setFont(BODY.deriveFont(Font.BOLD, 12f));
        jobTable.getTableHeader().setBackground(Color.WHITE);
        jobTable.getTableHeader().setForeground(MUTED);
        int[] widths = {115, 115, 130, 215, 145};
        for (int i = 0; i < widths.length; i++) {
            jobTable.getColumnModel().getColumn(i).setPreferredWidth(widths[i]);
        }

        JScrollPane resultsPane = new JScrollPane(jobTable);
        resultsPane.setColumnHeaderView(jobTable.getTableHeader());
        resultsPane.setBorder(BorderFactory.createEmptyBorder());
        resultsPane.getViewport().setBackground(Color.WHITE);
        resultsPane.setPreferredSize(new Dimension(750, 190));

        // Show instructions before the first preview; previewJob() switches to the table.
        JPanel emptyState = transparentPanel(new GridBagLayout());
        emptyState.setBorder(new RoundBorder(true));
        JPanel emptyText = transparentPanel(null);
        emptyText.setLayout(new BoxLayout(emptyText, BoxLayout.Y_AXIS));
        JLabel emptyIcon = new JLabel(icon("document", 36, new Color(150, 164, 192)));
        JLabel emptyTitle = label("Your job previews will appear here", 16, Font.BOLD, INK);
        JLabel emptyHint = label("Choose an operation, add data, then validate your input.", 13, Font.PLAIN, MUTED);
        emptyTitle.setHorizontalAlignment(SwingConstants.CENTER);
        emptyHint.setHorizontalAlignment(SwingConstants.CENTER);
        emptyIcon.setAlignmentX(Component.CENTER_ALIGNMENT);
        emptyTitle.setAlignmentX(Component.CENTER_ALIGNMENT);
        emptyHint.setAlignmentX(Component.CENTER_ALIGNMENT);
        emptyText.add(emptyIcon);
        emptyText.add(Box.createVerticalStrut(10));
        emptyText.add(emptyTitle);
        emptyText.add(Box.createVerticalStrut(5));
        emptyText.add(emptyHint);
        emptyState.add(emptyText);
        historyContent.setBackground(Color.WHITE);
        historyContent.setPreferredSize(new Dimension(750, 120));
        historyContent.add(emptyState, "empty");
        historyContent.add(resultsPane, "table");
        historyLayout.show(historyContent, "empty");
        history.add(historyContent, BorderLayout.CENTER);
        workspace.add(history, BorderLayout.CENTER);
        JScrollPane workspaceScroll = new JScrollPane(workspace,
                JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED, JScrollPane.HORIZONTAL_SCROLLBAR_NEVER);
        workspaceScroll.setBorder(BorderFactory.createEmptyBorder());
        workspaceScroll.getViewport().setBackground(BACKGROUND);
        workspaceScroll.getVerticalScrollBar().setUnitIncrement(20);
        root.add(workspaceScroll, BorderLayout.CENTER);
        statusLabel.setText("Ready to prepare a job.");
        statusLabel.setFont(BODY.deriveFont(12f));
        statusLabel.setForeground(MUTED);
        statusLabel.setIcon(icon("dot", 18, new Color(31, 178, 141)));
        statusLabel.setIconTextGap(8);
        JPanel footer = transparentPanel(new BorderLayout(16, 0));
        footer.add(statusLabel, BorderLayout.CENTER);
        JLabel connectionHint = label("Preview only. Results require a backend connection.",
                12, Font.PLAIN, MUTED);
        connectionHint.setIcon(icon("info", 17, MUTED));
        connectionHint.setIconTextGap(7);
        footer.add(connectionHint, BorderLayout.EAST);
        root.add(footer, BorderLayout.SOUTH);

        // Event handlers connect user actions to behaviour; building buttons alone does not.
        // Clearing input deliberately retains the preview history.
        jobTypeBox.addActionListener(event -> updateExample());
        loadCsvButton.addActionListener(event -> loadCsv());
        previewButton.addActionListener(event -> previewJob());
        clearButton.addActionListener(event -> {
            inputArea.setText("");
            statusLabel.setText("Input cleared. Your previews are retained.");
            inputArea.requestFocusInWindow();
        });

        updateExample();
        setSize(1120, 870);
        setLocationRelativeTo(null);
    }

    /** Groups components without covering the parent panel's background colour. */
    private static JPanel transparentPanel(LayoutManager layout) {
        JPanel panel = new JPanel(layout);
        panel.setOpaque(false);
        return panel;
    }

    /** Applies the common font family and requested emphasis to a text label. */
    private static JLabel label(String text, int size, int style, Color color) {
        JLabel label = new JLabel(text);
        label.setFont(BODY.deriveFont(style, (float) size));
        label.setForeground(color);
        label.setBorder(BorderFactory.createEmptyBorder(0, 0, 0, 18));
        label.setMaximumSize(new Dimension(Integer.MAX_VALUE, label.getPreferredSize().height));
        return label;
    }

    /** Shared section heading: an icon, title and a short explanation. */
    private static JPanel sectionHeading(String title, String description, String symbol) {
        JPanel heading = transparentPanel(new BorderLayout(14, 0));
        heading.add(badge(symbol), BorderLayout.WEST);
        JPanel text = transparentPanel(null);
        text.setLayout(new BoxLayout(text, BoxLayout.Y_AXIS));
        text.add(label(title, 20, Font.BOLD, INK));
        text.add(Box.createVerticalStrut(3));
        text.add(label(description, 13, Font.PLAIN, MUTED));
        heading.add(text, BorderLayout.CENTER);
        return heading;
    }

    /** Updates input guidance when the operation changes; preserves the user's text. */
    private void updateExample() {
        String operation = (String) jobTypeBox.getSelectedItem();
        inputArea.setHint("PRIMESUM".equals(operation)
                ? "Enter start and end, separated by a comma..."
                : "Enter integers separated by commas...");

        if ("PRIMESUM".equals(operation)) {
            exampleLabel.setText(
                    "Enter start,end (inclusive). Example: 1,1000"
            );
        } else if ("PRIMECOUNT".equals(operation)) {
            exampleLabel.setText(
                    "Enter integers. Example: 2,3,4,5,5"
            );
        } else {
            exampleLabel.setText(
                    "Enter integers. Example: 3,7,12,45"
            );
        }
    }

    /**
     * Opens a file chooser, reads UTF-8 text and validates it for the selected operation.
     * Cancellation or invalid data leaves the previous input untouched.
     * Supports plain numeric CSV only, without headers or quoted fields.
     */
    private void loadCsv() {
        JFileChooser chooser = new JFileChooser();
        chooser.setFileFilter(
                new FileNameExtensionFilter("CSV files (*.csv)", "csv")
        );
        chooser.setAcceptAllFileFilterUsed(false);

        if (chooser.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) {
            return;
        }

        File file = chooser.getSelectedFile();

        try {
            // A small-file limit keeps this initial GUI-only loader manageable.
            if (Files.size(file.toPath()) > 1_000_000) {
                throw new IllegalArgumentException(
                        "Please select a CSV file smaller than 1 MB."
                );
            }

            String contents = Files.readString(
                    file.toPath(), StandardCharsets.UTF_8
            );

            // Remove an optional UTF-8 byte-order mark.
            if (contents.startsWith("\uFEFF")) {
                contents = contents.substring(1);
            }

            // Validate before replacing the user's existing input.
            parseInput(contents);

            inputArea.setText(contents);
            statusLabel.setText(
                    "Loaded " + file.getName() + " — not submitted"
            );
        } catch (IOException exception) {
            showError("Could not read the selected file.");
        } catch (IllegalArgumentException exception) {
            showError(exception.getMessage());
        }
    }

    /**
     * Shared validation for manual entry and CSV loading.
     * Converts comma-separated integer cells across rows into a long array.
     * Throws IllegalArgumentException for bad input; callers display its message.
     * PRIMESUM additionally requires exactly two endpoints in ascending order.
     */
    private long[] parseInput(String text) {
        String cleaned = text.strip();

        if (cleaned.isEmpty()) {
            throw new IllegalArgumentException(
                    "Enter numbers or load a CSV file first."
            );
        }

        if (cleaned.contains("\"")) {
            throw new IllegalArgumentException(
                    "Use plain numeric CSV without quotes or headers."
            );
        }

        // Accept comma-separated values across multiple rows.
        String[] rows = cleaned.split("\\R", -1);
        java.util.List<Long> values = new java.util.ArrayList<>();

        for (String row : rows) {
            if (row.isBlank()) {
                throw new IllegalArgumentException(
                        "Remove empty rows from the input."
                );
            }

            // Keep trailing empty cells so inputs such as "3,8," are rejected.
            for (String cell : row.split(",", -1)) {
                String value = cell.strip();

                if (value.isEmpty()) {
                    throw new IllegalArgumentException(
                            "A value is missing between commas or at a row end."
                    );
                }

                try {
                    values.add(Long.parseLong(value));
                } catch (NumberFormatException exception) {
                    throw new IllegalArgumentException(
                            "Every value must be a whole number within "
                                    + "the supported 64-bit integer range."
                    );
                }
            }
        }

        String operation = (String) jobTypeBox.getSelectedItem();

        if ("PRIMESUM".equals(operation)) {
            if (values.size() != 2) {
                throw new IllegalArgumentException(
                        "PRIMESUM requires exactly two values: start,end."
                );
            }

            if (values.get(0) > values.get(1)) {
                throw new IllegalArgumentException(
                        "The start must be less than or equal to the end."
                );
            }
        }

        // Convert the temporary growable list into the numeric array returned to the caller.
        long[] result = new long[values.size()];

        for (int i = 0; i < values.size(); i++) {
            result[i] = values.get(i);
        }

        return result;
    }

    /**
     * Validates current input, appends a preview and updates the count/status.
     * No RMI request occurs here: backend submission and result handling are still missing.
     */
    private void previewJob() {
        try {
            long[] values = parseInput(inputArea.getText());
            String operation = (String) jobTypeBox.getSelectedItem();

            String inputSummary = "PRIMESUM".equals(operation)
                    ? values[0] + " to " + values[1]
                    : values.length + " numbers";

            jobModel.addRow(new Object[]{
                    "Preview-" + nextReference++,
                    operation,
                    inputSummary,
                    "Validated — not submitted",
                    "Awaiting backend"
            });
            historyLayout.show(historyContent, "table");
            int count = jobModel.getRowCount();
            previewCount.setText(count + (count == 1 ? " preview" : " previews"));

            statusLabel.setText(
                    "Input validated. No computation has been submitted."
            );
        } catch (IllegalArgumentException exception) {
            showError(exception.getMessage());
        }
    }

    /** Displays validation and file-reading errors in a dialog attached to this window. */
    private void showError(String message) {
        JOptionPane.showMessageDialog(
                this,
                message,
                "Input error",
                JOptionPane.ERROR_MESSAGE
        );
    }

    public static void main(String[] args) {
        // Create Swing components on the Event Dispatch Thread, which handles UI events.
        // Future network calls and long calculations must run elsewhere to avoid freezing it.
        SwingUtilities.invokeLater(
                () -> new ClientApp().setVisible(true)
        );
    }
}
