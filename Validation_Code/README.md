This folder contains the data and notebooks used to validate the CIDRE algorithm.

Folder info:

"Data": Contains data files such as edge files for the 2013 and 2020 journal networks and JCR suppressions for validating CIDRE.

"Validation Notebooks": Contains a "Replicate_Experiments" Python notebook that applies CIDRE to a cleaned dataset created by the original authors using the MAG data set for 2013 journals. It also contains a "Reproduce_Experiments" Python notebook that applies CIDRE to the new, unseen data of 2020 journals that was collected and cleaned by myself (Gregory Lister Pollard). Both notebooks were created in Google Colaboratory and are recommended to be uploaded adn executed on this platform.

The Google Colaboratory runtime setting requirements include:

- "High-RAM"
- "TPU"