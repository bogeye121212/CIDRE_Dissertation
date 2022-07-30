This folder contains the data and notebooks used to extend the CIDRE algorithm.

Folder info:

"Data": Contains data files the JCR cartels for evaluation and synthetic data sets generated.

"Extending_Notebooks": Contains Python notebooks on the processes for generating the synthetic data, developing the improvements, and evaluating these improvements against vanilla CIDRE.

The Google Colaboratory runtime setting requirements include:

- "High-RAM"
- "TPU"

"gen_syntetiic_data.py": A python file writiten by the original authors of CIDRE used as the framework for generating synthetic data in the aforementioned notebooks. Source: https://github.com/skojaku/cidre/tree/main/data/synthe