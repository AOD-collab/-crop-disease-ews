# Drop your trained model here

This folder is the integration point. Once you've trained a model in the
separate `maize_model_training/` project and run its `export_model.py`,
copy the resulting file here:

```
ml/trained_model/saved_model_cnn.pt
```

That's it — `ml/inference.py` picks it up automatically, and
`spark/streaming_job.py` uses it for every real Android/API image upload.

If you keep the file elsewhere, point `MODEL_PATH` at it instead:

```bash
export MODEL_PATH=/path/to/your/saved_model_cnn.pt
```

No file here? Predictions fail loudly with `ModelNotAvailable` (DIT manual
section 24: "If the model is unavailable, return a clear model-not-found
error"). There is no fallback analyzer: every result comes from the model.
A real trained checkpoint already ships at `saved_model_cnn.pt` in this
folder (classes: `common_rust`, `gray_leaf_spot`, `healthy`,
`northern_leaf_blight`), so this only matters if you replace or remove it.
