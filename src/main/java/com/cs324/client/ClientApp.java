package com.cs324.client;

import javax.swing.*;
import java.awt.*;
/**
 * Entry point for the Client GUI.
 * Lets the user enter data manually or load a CSV, submit MAX / PRIMESUM /
 * PRIMECOUNT jobs to the current coordinator, and run multiple jobs
 * concurrently. Multiple client instances may run at once.
 */

public class ClientApp extends JFrame {

    public ClientApp() {
        super("CS324 Distributed Job Client");
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(600, 500);
        setLocationRelativeTo(null);

        //Job type selector
        JPanel topPanel = new JPanel();
        topPanel.add(new JLabel("Job Type:"));
        topPanel.add(new JLabel("Data:"));
        JTextField dataField = new JTextField(20);
        topPanel.add(dataField); 
        JComboBox<String> jobTypeBox = new JComboBox<>(new String[]{"MAX", "PRIMESUM", "PRIMECOUNT"});
        topPanel.add(jobTypeBox);
        add(topPanel, BorderLayout.NORTH);

        //Manual entry input area
        JPanel inputPanel = new JPanel(new BorderLayout());
        inputPanel.setBorder(BorderFactory.createTitledBorder("Manual Input"));
        JTextField inputField = new JTextField("e.g. 3,7,12,45 or start,end for PRIMESUM");
        inputPanel.add(inputField, BorderLayout.CENTER);

        //CSV file input area
        JButton loadCsvButton = new JButton("Load CSV");
        inputPanel.add(loadCsvButton, BorderLayout.EAST);

        //Submit button
        JButton submitButton = new JButton("Submit Job");
        JPanel actionPanel = new JPanel();
        actionPanel.add(submitButton);
        add(actionPanel, BorderLayout.SOUTH);

            // Combine input and action panels for looks
            JPanel centerTop = new JPanel(new BorderLayout());
            centerTop.add(inputPanel, BorderLayout.CENTER);
            centerTop.add(actionPanel, BorderLayout.SOUTH);

        //Add results display area
        JTextArea resultsArea = new JTextArea();
        resultsArea.setEditable(false);
        resultsArea.setBorder(BorderFactory.createTitledBorder("Results"));
        JScrollPane resultsScroll = new JScrollPane(resultsArea);

            //Combine all into one frame
            JPanel centerPanel = new JPanel(new BorderLayout());
            centerPanel.add(centerTop, BorderLayout.NORTH);
            centerPanel.add(resultsScroll, BorderLayout.CENTER);
            add(centerPanel, BorderLayout.CENTER);
        
        //Status bar
        JLabel statusLabel = new JLabel("Status: Not connected");
        statusLabel.setBorder(BorderFactory.createEmptyBorder(5, 10, 5, 10));
        add(statusLabel, BorderLayout.SOUTH);
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> new ClientApp().setVisible(true));
    }
}
