# The deployed model

`saved_model_cnn.pt` in this folder is the CNN the system uses: an
SE-ResNet-18 trained in the separate `crop_disease_ews` training project
(`TRAINING_PROJECT_DIR`), with classes `common_rust`, `gray_leaf_spot`,
`healthy` and `northern_leaf_blight`.

To replace it, train from the dashboard's **Model training** page. A new
model that beats the one in use is copied here automatically (the old file
goes to `backups/`), or you can deploy one by hand from that page.
`ml/inference.py` reloads the file when it changes, so the Spark job uses
the new model without a restart.

The checkpoint also carries the input the model expects (`img_size`,
`mean`, `std`). Spark prepares every photo with those values
(`spark/preprocessing.model_input`), so a retrained model with other values
needs no code change.

If you keep the file elsewhere, point `MODEL_PATH` at it instead:

```bash
export MODEL_PATH=/path/to/your/saved_model_cnn.pt
```

No file here? Predictions fail loudly with `ModelNotAvailable` (DIT manual
section 24: "If the model is unavailable, return a clear model-not-found
error"). There is no fallback analyzer: every result comes from the model.
