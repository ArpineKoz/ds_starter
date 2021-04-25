"""
This file launches the analysis routine for anomaly calculation
We set here whether we want to do pre-processing (to confirm if we need or not), run the target derivation pipeline described in the doc file, 
run the training of the autoencoder or make a prediction with autoencoder.
pipeline=1, 2, 3, 4

based on the choice it then calls the corresponding operation.
"""

#'plant_1', 'plant_2"
plant='plant_1'

#1=preprocess
#2=get target
#3=train autoencoder
#4=predict autoencoder
#5=compare performances
#[preprocess, prepare, train, predict, compare performances]
#or
#[1,2,3,4,5]
operations=["prepare"]

for operation in operations:
    # call operation_prepare.py and pass plant to it